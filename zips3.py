#*****************************************************************************************
# zips3.py - https://gh.mukunda.com/zips3
#
# (C) 2021 Mukunda Johnson (mukunda.com)
#
# Exports files from S3 and packs them into zip archives.
#
# Goals:
#   Speedy - Threaded downloading with low memory usage.
#   In-place - Pipelining the whole operation to do downloading and packing in tandem.
#   Robust - Ability to resume large jobs, halts if detecting a network error, automatic
#             retry on transient failures.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
# OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#-----------------------------------------------------------------------------------------
# A little bit about automatic retries...
# boto3 supports automatic retrying - you can configure it with your .aws config file.
# See here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
# Default is "legacy" retry mode with 5 max attempts.
#/////////////////////////////////////////////////////////////////////////////////////////
import sys, re, os, threading, uuid, json, queue, tarfile, zipfile, datetime, shutil
import botocore, boto3

VERSION = "0.9"

#-----------------------------------------------------------------------------------------
# One lock to rule them all. Use for any Python library function that you think might not
#  be thread-safe. And watch out for deadlocks. :)
python_mutex = threading.Lock()

#-----------------------------------------------------------------------------------------
# Overriding print to be thread safe.
original_print = print
def print( *args, **kwargs ):
   with python_mutex:
      original_print( *args, **kwargs )

##########################################################################################
# Basic logger.
##########################################################################################
class Logger():
   ERROR = 0
   WARN  = 1
   INFO  = 2
   TRACE = 3

   LEVEL_STRINGS = [
      "ERROR", "WARN", "INFO", "TRACE"
   ]

   #--------------------------------------------------------------------------------------
   # path = file path to use. If null, this logger will only print to the screen.
   # level = lowest logger level that will be shown.
   def __init__( self, path, level = None ):
      if level == None: level = Logger.WARN
      self.fp = None
      if path:
         self.fp = open( path, "a", encoding="utf-8" )
      self.level = level

   #--------------------------------------------------------------------------------------
   # Close the file handle. Called automatically on delete.
   def close( self ):
      if self.fp:
         self.fp.close()
         self.fp = None

   #--------------------------------------------------------------------------------------
   def __enter__( self ): return self
   def __exit__( self, exception_type, exception_value, exception_traceback ):
      self.close()
   #--------------------------------------------------------------------------------------
   def __del__( self ): self.close()

   #--------------------------------------------------------------------------------------
   # Returns a timestamp.
   def get_timestamp( self ):
      # I'm not sure if datetime -> string conversion is thread safe.
      with python_mutex:
         return str(datetime.datetime.now())

   #--------------------------------------------------------------------------------------
   # Add a message to the log.
   def log( self, level, *args ):
      if level > self.level: return
      
      timestamp = self.get_timestamp()

      # [timestamp] [thread id] [level] [message]
      prefix = f"[{timestamp}] [{threading.currentThread().ident}] {Logger.LEVEL_STRINGS[level]}"
      if self.fp:
         # Might be a -bit- inefficient. From past experience, I believe print can be
         #  pretty slow.
         print( prefix, *args, file = self.fp )

      # Don't spam console with trace unless we are using the screen logger.
      #if level != Logger.TRACE or not self.fp:
      print( prefix, *args ) #DEBUG BYPASS
   
   #--------------------------------------------------------------------------------------
   # Convenience wrappers.
   def error( self, *args ): self.log( Logger.ERROR, *args )
   def warn( self, *args ): self.log( Logger.WARN, *args )
   def info( self, *args ): self.log( Logger.INFO, *args )
   def trace( self, *args ): self.log( Logger.TRACE, *args )

default_logger = Logger( None, Logger.INFO )

##########################################################################################
# S3 Multithreaded Downloader
##########################################################################################
class Downloader():
   
   #--------------------------------------------------------------------------------------
   def __init__( self, s3_client, download_thread_count = 20, download_queue_size = 20,
                                                                logger = default_logger ):
      self.s3_client             = s3_client
      self.download_thread_count = download_thread_count

      # Queue size is much larger than the thread count. The threads are the consumers
      #      and we don't want to hold up other components when there are free resources.
      self.downloader_queue      = queue.Queue( download_queue_size )
      self.downloader_threads    = []
      self.mut                   = threading.Lock()
      self.logger                = logger
      self.logger.trace( f"Initialized downloader. Thread count={download_thread_count}" )
      self.start()

   #--------------------------------------------------------------------------------------
   def __enter__( self ): return self
   def __exit__( self, exception_type, exception_value, exception_traceback ):
      self.stop()
   
   #--------------------------------------------------------------------------------------
   # Downloader thread process.
   def downloader_thread( self ):
      self.logger.trace( "Download thread started.", threading.get_ident() )
      while True:
         # We're not utilizing the 'tasks' in our workflow, so we'll just mark a task
         #  complete on every pull.
         file = self.downloader_queue.get()
         self.downloader_queue.task_done()

         if file == "#queue-end":
            # This is a special flag that tells us the parent has no more work to process.
            #  We'll put this flag back in the queue for the next thread to receive it.
            #  The parent will pop the flag when all threads have exited.
            #  (see self.close())
            self.downloader_queue.put( file )
            break

         s3_path = f"s3://{file['bucket']}/{file['path']}"

         try:
            self.logger.trace( f"Downloading... {s3_path}" )

            # s3_client is supposed to be thread safe!
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
            # There are a few stipulations, but they don't affect us.
            self.s3_client.download_file( file["bucket"], file["path"], file["local_path"] )

            self.logger.trace( f"Download complete: {s3_path}" )

            if file["on_complete"]: file["on_complete"]( file, "complete" )

         except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == "404":
               # File not found error.
               #
               # This is OK - it just means that we got a listing file when that file used
               #  to exist. Just log a warning and continue.
               self.logger.warn( f"Encountered 404 for {s3_path}" )
               if file["on_complete"]: file["on_complete"]( file, "notfound" )
            else:
               # HACK: This class shouldn't have responsiblity of terminating the program.
               # Boto3 supports automatic retries. If there is an error here, we can 
               #  assume it is fatal. They need to 
               self.logger.error(
                 f"Encountered error when downloading {s3_path}. Terminating execution." )
               self.logger.error( error )
               sys.exit( -100 )
         
      self.logger.trace( "Download thread ending.", threading.get_ident() )

   #--------------------------------------------------------------------------------------
   def __del__( self ):
      self.stop()
      
   #--------------------------------------------------------------------------------------
   # Start the downloader threads.
   def start( self ):
      for i in range( 0, self.download_thread_count ):
         t = threading.Thread( None, lambda: self.downloader_thread(), None )
         t.start()
         self.downloader_threads.append( t )
         
   #--------------------------------------------------------------------------------------
   def stop( self ):
      # Wait until the queue is empty and send the termination signal. The threads will
      #  re-queue the terminator, propagating it and quitting.
      self.logger.trace( "Stopping downloader instance. Joining queue." )
      self.downloader_queue.join()
      self.downloader_queue.put( "#queue-end" )
      
      self.logger.trace( "Queue has finished. Waiting for threads to exit." )
      # Join all threads to wait until they end.
      for thread in self.downloader_threads:
         thread.join()
      
      # Pop the termination signal.
      self.downloader_queue.get()
      self.downloader_queue.task_done()
      self.logger.trace( "Downloader threads ended." )

   #--------------------------------------------------------------------------------------
   def download_file( self, bucket, path, local_path, on_complete = None ):
      self.logger.trace( f"Queueing for download... s3://{bucket}/{path} -> {local_path}" )
      self.downloader_queue.put({
         "bucket"      : bucket, 
         "path"        : path,
         "local_path"  : local_path,
         "on_complete" : on_complete
      })
         
##########################################################################################
# A very basic promise. Or is this a "future"? I don't know.
##########################################################################################
class Promise():
   def __init__( self ):
      self.value = None
      # We'll use threading.event to signal threads when the value is ready.
      self.event = threading.Event() 
   
   #--------------------------------------------------------------------------------------
   # Wait until the value is available and return it.
   def resolve( self ):
      self.event.wait()
      return self.value
      
   #--------------------------------------------------------------------------------------
   # Set the value of this promise and allow resolving.
   def set( self, value ):
      self.value = value
      self.event.set()
      
##########################################################################################
# Resolver Queue
# 
# This is a class that inputs a sequence of promise items and resolves and processes
#  them in synchronous order.
#
# The goal of the resolver queue in our scenario is to download items asynchronously but
#  pack them in-order.
##########################################################################################
class ResolveQueue():
   #--------------------------------------------------------------------------------------
   def __init__( self, max_size = 20 ):
      self.queue = queue.Queue( max_size )
      self.worker_thread = False
      
   #--------------------------------------------------------------------------------------
   def __del__( self ):
      self.close()
   
   #--------------------------------------------------------------------------------------
   def thread( self ):
      while True:
         item = self.queue.get()
         self.queue.task_done()
         if item == "#queue-end":
            break
            
         processor = item.resolve()
         if processor.get( "run", None ):
            processor["run"]( processor )

   #--------------------------------------------------------------------------------------
   def join( self ):
      self.queue.join()
   
   #--------------------------------------------------------------------------------------
   def add( self, item ):
      self.start()
      self.queue.put( item )
      
   #--------------------------------------------------------------------------------------
   def start( self ):
      if self.worker_thread: return
      self.worker_thread = threading.Thread( None, self.thread, None )
      self.worker_thread.start()
      
   #--------------------------------------------------------------------------------------
   def close( self ):
      if not self.worker_thread: return
      # Send a terminator and join the queue until it is done.
      self.queue.put( "#queue-end" )
      self.worker_thread.join()
      self.worker_thread = None
      
##########################################################################################
# Archive Packer
##########################################################################################
class Packer():
   #--------------------------------------------------------------------------------------
   def __init__( self, name, output_directory, tempfile = "packer_tmp.zip",
                               max_archive_size = 50*1024*1024, logger = default_logger ):
      self.output_name      = name
      self.output_directory = output_directory
      self.tempfile         = tempfile
      self.max_archive_size = max_archive_size
      self.callbacks        = {}
      self.output           = None
      self.logger           = logger

      if tempfile.endswith( ".tar.gz" ):
         self.suffix = ".tar.gz"
      elif tempfile.endswith( ".zip" ):
         self.suffix = ".zip"

   #--------------------------------------------------------------------------------------
   # Don't need to call this manually - it will be called when you add files.
   def start( self ):
      if self.output: return
      
      # Delete incomplete attempt.
      if os.path.exists( self.tempfile ):
         os.remove( self.tempfile )
      
      self.logger.trace( "Opening new archive for writing." )
      if self.suffix == ".zip":
         self.output = zipfile.ZipFile( self.tempfile, "w",
                                   compression = zipfile.ZIP_DEFLATED, compresslevel = 9 )
      elif self.suffix == ".tar.gz":
         self.output = tarfile.open( self.tempfile, "w|gz", encoding = "utf-8" )

   #--------------------------------------------------------------------------------------
   # Returns the file write position for the currently open archive.
   def current_size( self ):
      if self.suffix == ".zip":
         return self.output.fp.tell()
      elif self.suffix == ".tar.gz":
         return self.output.fileobj.tell()

   #--------------------------------------------------------------------------------------
   # Add a file to the archive. file_path is a path on disk. arc_path is where to store it
   #  in the archive.
   def add( self, file_path, arc_path ):
      self.start()

      self.logger.trace( "Adding file to archive.", file_path, "->", arc_path )
      if self.suffix == ".zip":
         self.output.write( file_path, arcname = arc_path )
      elif self.suffix == ".tar.gz":
         self.output.add( file_path, arcname = arc_path )
      os.remove( file_path )

      if self.current_size() > self.max_archive_size:
         self.close()

   #--------------------------------------------------------------------------------------
   # Format an output archive path on disk from the index given.
   def get_output_path( self, index ):
      return f"{self.output_directory}/{self.output_name}_{index:02d}{self.suffix}"
      
   #--------------------------------------------------------------------------------------
   # Scans the output directory and finds a filename that is free.
   def get_next_output_path( self ):
      i = 1
      while os.path.exists( self.get_output_path(i) ):
         i += 1
      return self.get_output_path(i)
      
   #--------------------------------------------------------------------------------------
   # Close the current archive and save it to the output directory.
   def close( self ):
      if not self.output: return
      self.output.close()
      self.output = None
      
      output_path = self.get_next_output_path()
      self.logger.info( "Saving archive.", output_path )
      os.rename( self.tempfile, output_path )
      
      if "close" in self.callbacks:
         self.callbacks["close"]( self )
         
   #--------------------------------------------------------------------------------------
   def set_callback( self, name, func ):
      self.callbacks[name] = func
      if not func: del self.callbacks[name]
      
##########################################################################################
# Main
##########################################################################################

#-----------------------------------------------------------------------------------------
# Load the JSON file from the path and strip out any comment strings.
# Very basic detection - the comments must be the only thing on the line.
def load_json_with_comments( path ):
   with open( path, "r", encoding="utf-8" ) as f:
      content = f.read()
      content = re.sub( r"^\s*//.*", "", content, flags = re.MULTILINE )
      return json.loads( content )

#-----------------------------------------------------------------------------------------
class Job():
   #--------------------------------------------------------------------------------------
   def __init__( self, config ):
      self.job           = self.get_config( config )
      if not self.job: return

      self.callbacks     = {}
      self.working_directory = self.job.get( "working_directory", "zips3-staging" )
      self.start_index   = 0
      self.output_folder = self.job.get( "output_folder",
                                                      self.working_directory + "/output" )
      self.dltmp_folder = self.working_directory + "/dltmp-" + self.job["name"]
      os.makedirs( self.working_directory, exist_ok = True )
      os.makedirs( self.output_folder, exist_ok = True )

      # Clean up any existing dltmp folder.
      if os.path.exists( self.dltmp_folder ):
         shutil.rmtree( self.dltmp_folder )
      os.makedirs( self.dltmp_folder, exist_ok = True )
      self.logger = Logger( self.working_directory + f"/{self.job['name']}.log", Logger.TRACE ) #DEBUG

      aws_profile = self.job.get("aws-profile")

      # In case someone passes an empty string to the config, treat that as None.
      if not aws_profile: aws_profile = None

      # Sessions aren't supposed to be thread safe, but the s3_client is thread safe.
      #                      s3_client is the only thing used in the threaded operations.
      self.boto_session = boto3.Session( profile_name = aws_profile )
      self.s3_resource  = self.boto_session.resource('s3')
      self.s3_client    = self.boto_session.client('s3')

   #--------------------------------------------------------------------------------------
   # Read a job configuration file with some basic error handling.
   def get_config( self, path ):
      try:
         # Strip comments.
         return load_json_with_comments( path )
      except json.decoder.JSONDecodeError as err:
         # Doing "print" here for errors, since the log file location is not initialized
         #  yet.
         print( "Error loading job configuration.", path )
         print( err )

   #--------------------------------------------------------------------------------------
   # Gets the file path for the progress json file.
   def get_progress_file_path( self ):
      # We use names based off of the job in case the user wants to handle multiple jobs
      #             in the same stage directory, especially if they are running in tandem.
      return self.working_directory + f"/progress-{self.job['name']}.json"
   
   #--------------------------------------------------------------------------------------
   # Return the currently save progress data for this class.
   def load_progress( self ):
      if not os.path.exists( self.get_progress_file_path() ):
         self.logger.trace( "Loading progress: fresh run" )
         # Not found - let's return a 'fresh' template.
         return {
            "index": 0
         }
      
      with open( self.get_progress_file_path(), "r", encoding="utf-8" ) as f:
         try:
            self.logger.trace( "Loading progress." )
            return json.load( f )
         except Exception as e:
            # Anything unexpected should trigger a fatal error so that it can be
            #  addressed.
            self.logger.error( f"The progress file {self.get_progress_file_path()} has "
                               f"an error. Delete it to start over." )
            self.logger.error( e )
            sys.exit( -102 )

   #--------------------------------------------------------------------------------------
   # Save the current progress to disk. This happens after an archive slice is
   #  completed. If something interrupts the process, then we can resume after the last
   #  checkpoint.
   def save_progress( self, file_line_index ):
      self.logger.trace( "Saving progress:", file_line_index )
      with open( self.get_progress_file_path(), "w", encoding="utf-8" ) as f:
         json.dump( {
            "index": file_line_index
         }, f, indent=2 )

   #--------------------------------------------------------------------------------------
   # Get a file list from S3 according to the source provided. Source correlates directly
   #                                                     to a "source" in the config JSON.
   def add_source_listing( self, source, output_file ):
      self.logger.info( f"Getting listing for s3://{source['bucket']}/{source['prefix']}" )
      
      my_bucket = self.s3_resource.Bucket( source["bucket"] )
      
      # Convert each pattern string to a compiled regex. `patterns` will contain a tuple
      # with the pattern and inclusion option (true or false)
      self.logger.trace( "Converting patterns to regex." )
      patterns = source.get( "patterns", [] ).copy()
      for index, text in enumerate(patterns):
         option, pattern = text.split("=", 1)
         option = option.strip().lower()
         if option == "include" or option == "exclude":
            patterns[index] = (re.compile(pattern), True if option == "include" else False)
         else:
            self.logger.error( "Invalid pattern option:", option )
            sys.exit( -101 )
      
      total     = 0
      counted   = 0
      prefixlen = len(source.get("prefix", ""))

      output_file.write( "@@source=" + json.dumps( source ) + "\n" )
      for obj in my_bucket.objects.filter( Delimiter=source.get("delimiter", ""),
                                          Prefix=source.get("prefix", "") ):
         shortkey = obj.key[prefixlen:]
         total += 1
         included = True

         # Iterate through the patterns to determine if this file is included.
         # Each successful check overwrites the previous result. There's no special
         #  merging logic.
         for pattern, option in patterns:
            if pattern.search( shortkey ):
               self.logger.trace( shortkey, "matched against", pattern,
                                                    "included =", option )
               included = option
         
         if included:
            counted += 1
            output_file.write( shortkey + "\n" )

         # "limit" is used mainly for testing, to do a short test-run and pack some
         #  archives to see, for example, if there is proper permission or paths being
         #  used. Then afterwards, the full export is performed.
         if source.get( "limit" ):
            if total >= source["limit"]:
               self.logger.warn( f"Quitting job listing with limit ({source['limit']}) specified." )
               break
      
      # Basic diagnostics.
      self.logger.info( f"{counted} files marked for download." )
      self.logger.info( f"{total} total files scanned." )

   #--------------------------------------------------------------------------------------
   # Returns the file listing path for this job.
   def get_file_listing_path( self ):
      # file_listing_{jobname}
      return f"{self.working_directory}/file_listing_{self.job['name']}"

   #--------------------------------------------------------------------------------------
   # Iterate through the source sections and download the full file listing, compiling
   #  the listing file.
   def download_file_listing( self ):
      with open( self.get_file_listing_path(), "w" ) as f:
         for source in self.job["sources"]:
            self.add_source_listing( source, f )
            
   #--------------------------------------------------------------------------------------
   # Process the job (main function).
   def process( self ):
      if not self.job:
         # The job could not be loaded - terminate.
         return

      self.logger.info( "Processing job:", self.job["name"] )
      
      # This downloader will be used for everything. Pass in our s3_client that we
      #  allocated.
      with Downloader( self.s3_client,
                      download_thread_count = self.job.get( "download_thread_count", 20 ),
                      logger = self.logger ) as downloader:
         self.downloader = downloader 

         progress = self.load_progress()

         # This flag in the progress file indicates this was already completed.
         if progress["index"] == "#complete":
            # Only an error for visiblity and telling the user the job was terminated.
            # I guess that is technically an error.
            self.logger.error( "Job is already complete." )
            # Exit early.
            return

         self.start_index = progress["index"]

         # start_index holds the index of the next file we need to download. It's only
         #              nonzero if the job was previously interrupted and we are resuming.
         if self.start_index == 0:
            self.download_file_listing() 
            
         self.logger.info( "Processing file listing..." )
         with open( self.get_file_listing_path(), "r" ) as file_listing:
         
            # The tempfile should be based on the job name to avoid conflicts with other
            #  jobs.
            self.packer = Packer(
               name             = self.job["name"],
               output_directory = self.output_folder,
               tempfile         = f"{self.working_directory}/tmp_{self.job['name']}.zip",
               max_archive_size = self.job.get( "max_archive_size", 50000 ) * 1024 * 1024,
               logger           = self.logger )
               
            self.packer_queue = ResolveQueue()
            
            if self.start_index > 0:
               # Just informing the user. We still scan each line until the resume point
               #  in order to process metadata (it's quick).
               print( f"Resuming job at line {self.start_index}." )
            
            file_line_index = 0
            for file_line in file_listing:
               # Process each line in the listing file.
               file_line = file_line.strip()
               self.process_file( file_line, file_line_index )
               file_line_index += 1
            
            # Wait until all packer jobs are completed.
            self.packer_queue.close()

            # Then close the final archive. This won't do anything on the lucky occasion
            #                               that there are 0 files pending to be archived.
            self.packer.set_callback( "close", None )
            self.packer.close()

            # Write the #complete flag to the progress file to prevent another resume.
            self.save_progress( "#complete" )
   
   #--------------------------------------------------------------------------------------
   # Executed for each line in the listing file.
   # `file_line` is the text of the line stripped.
   # `file_line_index` is a zero-based line index.
   def process_file( self, file_line, file_line_index ):
      
      # The @@source tag contains a JSON dump of the current "source" section being
      #  processed. Mainly used to map the bucket and prefixes.
      if file_line[0:9] == "@@source=":
         self.current_source = json.loads( file_line[9:] )
         return

      # Skip comments and whitespace.
      if file_line[0] == '#' or file_line == "": return

      # Skip lines before our starting line. We still want to process the @@source tags
      #                      rather than outright skipping to the start line in the file. 
      if file_line_index < self.start_index: return
      
      # Each file line contains just the short path (prefix excluded) and will be combined
      #  with the current source info.
      shortpath = file_line 
      
      # Rather than storing the downloaded file temporarily as the S3 filename, we'll use
      #  a UUID to avoid any conflicts with existing files. Multiple jobs can be processed
      #  concurrently in the same working directory. (And plus this is simpler than using
      #  the S3 path.)
      output = f"{self.dltmp_folder}/{str(uuid.uuid4())}"
      
      # Compute the location in the archive where we will store this file.
      archive_path = self.current_source.get( "archived_path_prefix", "" ) + shortpath
      
      # This is our promise object that is resolved when the download is completed. It's
      #  placed into the resolver queue and then is used as a callback for the packer
      #  process.
      finished_promise = Promise()
      
      # Glad this worked out with my minimal understanding of Python closures.
      # Lots of closures here to capture the context of this function call.
      def on_download_complete( file_info, status ):

         # This is triggered when the packer closes an archive. We will capture the
         #  context of the current line index and save that to the progress file.
         # Basically, checkpoints are made after each zip file is saved. Anything between
         #  the checkpoint is discarded if a job is interrupted.
         def on_packer_close( packer ):
            self.save_progress( file_line_index )

         # `status` will be "complete" if a download is successful.
         if status == "complete":
            def do_packer_add( source ):
               # For each file packed, we're creating a fresh closure to catch the closed
               #  function, paired with the current index, simply so that we can save the
               #                                progress with the current file line index.
               self.packer.set_callback( "close", on_packer_close )
               self.logger.trace( f"Packing file... {file_line_index} - {archive_path}" )

               # This is the function that will call the callback above directly when the
               #  archive is split.
               self.packer.add( output, archive_path )

               self.logger.trace( f"Packing complete. {file_line_index} - {archive_path}" )
            
            # Pass our closure to the promise, and the resolve queue will run the packing
            #  operation in-order (so, regardless of download order [async], the files are
            #  always added in the file-listing order).
            finished_promise.set({
               "run": do_packer_add
            })
         else:
            # We failed to download a file. Currently this is only for 404 errors, where
            #  the file was removed from the bucket after previously being listed. Since
            #  we can't recover from that, we don't need user input to correct a problem.
            # A warning from the downloader instance is saved to the log.
            def do_packer_failed( source ):

               # We aren't using the callback currently.
               if self.callbacks["failure"]:
                  self.callbacks["failure"]({
                     "status": status,
                     "file_line_index": file_line_index,
                     "source": source
                  })
            
            finished_promise.set({
               "run": do_packer_failed
            })
      
      # Pass the context to the downloader. This will execute in an async environment.
      self.downloader.download_file( self.current_source["bucket"],
                                     self.current_source["prefix"] + shortpath, output,
                                                     on_complete = on_download_complete )
      # After the async download finishes, the magic above happens, resolves this promise,
      #  and then the packer will receive the archiving job.
      self.packer_queue.add( finished_promise )

   #--------------------------------------------------------------------------------------
   # Register a callback function. Not used currently.
   def set_callback( self, name, func ):
      self.callbacks[name] = func

#-----------------------------------------------------------------------------------------
# Print basic usage and link the GitHub repo.
def print_usage():
   print( f"*** zips3.py v{VERSION}***" )
   print( f"Usage: zips3 <job.json>" )
   print( f"Visit gh.mukunda.com/zips3 for further information." )

#-----------------------------------------------------------------------------------------
# Entry point.
def run():
   if len(sys.argv) < 2:
      print_usage()
      return
   
   # All options are passed in from the configuration file.
   config_file = sys.argv[1]

   job = Job( config_file )
   job.process()

if __name__ == "__main__": run()
#/////////////////////////////////////////////////////////////////////////////////////////
