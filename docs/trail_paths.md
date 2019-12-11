## The SnowShu Trail Path File
SnowShu runs use a single yaml configuration file (called a trailpath in SnowShu speak) to control most of the sampling behavior. There are only a handful of inputs to a run that do _not_ need to come from this trailpath. These include:
- cli flags and options
- credentials
- environment variables `SNOWSHU_CREDENTIALS_FILEPATH`
 

## Creating my `trail-path.yml` and `credentials.yml` files:
SnowShu comes with a helper method for creating sample files.

```
snowshu init .
```
where `.` is the current directory. You can have snowshu create sample `trail-path.yml` and `credentials.yml` files in any directory, as long as files with the same names do not already exist there. 
*Note:* it is not safe to store your `credentials.yml` file in source control as it contains sensitive information. You can locate the file at any path by changing the `credspath` value in your `trail-path.yml`



### What happens in my `trail-path.yml` file?

