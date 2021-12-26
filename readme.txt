zips3.py

A tool for downloading S3 files into zip archives. Not sure how useful it is compared to a few AWS CLI commands, but it's an interesting exercise.

Supports threaded downloading, automatic zip splitting, and low footprint (downloading and archiving done concurrently).

See example.job.json for the configuration.