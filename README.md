This is demo code for my presentation on Storm at GlueCon 2013. It counts unique tweets by person for a given search.

## Usage
You need to set two environment variables to use the Twitter Streaming API:

```bash
$ export TWITTER_USERNAME="your username"
$ export TWITTER_PASSWORD="your password"
$ ./gradlew run -Pargs='--filter #gluecon'
```
