==========================
Frequently Asked Questions
==========================
Why Can't I Change The Replica Username/Password/Port?
=========================================================
SnowShu is designed to make data devops as simple as possible. 
By making replicas "credential-less" we eliminate one of the more complex parts of 
the development workflow. We also wanted to remove any false sense of security; 
replicas are *highly insecure* and infosec policies should reflect this. Controlling access
to a replica is the only reliable way to protect the data inside.  

... But What If I Need To Run Multiple Replicas Side-By-Side?
-------------------------------------------------------------
Since replicas are docker images, the 9999 port can be
re-mapped as needed for multiple parallel replicas - so you could have one sample on 9999, 
a second one on 9998, and a third on 9997, all logging in with ``snowshu`` as the username and password. 

My Tests Pass In SnowShu But Fail In CI, Does SnowShu Not Work? 
===============================================================
This is covered in it_only_takes_one_, but TLDR; SnowShu is not intended to be the end-all testing for your data development. 
Check out our suggested_data_workflows_ for direction on how you can make the most of SnowShu in your data devops.  

Why Did You Name It SnowShu?
============================
We toiled over the name for a bit, but eventually settled on SnowShu because:

- Data are very often represented as "snowflakes" in technical terminology. This likely evolved from 
  the term *snowflake schema* and now reflects how tiny snowflakes can quickly pile up into city-crippling snowdrifts.
  When you have several feet of powder you need to cross quickly, snowshoes will let you skim over the surface without 
  sinking in. SnowShu does the same, but with massive volumes of data. 
- SnowShu not SnowShoe becuase Health Union (hu) was cool enough to support us building and sharing this much needed tool, and this is a nod to say thanks. 
- Some Engineer somewhere is tongue-tied trying to say "we sampled the Snowplow data from Snowflake in SnowShu." You're welcome :)


