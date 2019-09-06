# s3migrate
A miniature command-line tool for migrating entire buckets between S3-compatible
servers, such as S3-to-Jortage, Wasabi-to-DOSpaces, etc.

## Usage
Just execute `./s3migrate`. (Java must be installed.) The tool will walk you
through all the data it needs, and then print out a summary of what it's doing
until it finishes.

**Note**: It is unavoidable that all data must be downloaded locally and then
uploaded to the new server. There's no way to get an S3-compatible server to
post its data directly to another S3-compatible server. As such, this is no
more efficient than the usually-recommended recursive download followed by a
recursive upload, other than the fact it doesn't use any disk space, rather than
making you store a full copy of the entire bucket locally, which may be
impractical if you're storing hundreds of gigabytes.

**Also note**: Most third-party S3 servers don't support full ACLs, so neither
does s3migrate. Only "canned" ACLs will be copied; specifically, "private" and
"public-read". The rest will be stripped.
