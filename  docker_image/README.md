
# Docker SnowShu

A simple image for running SnowShu in a container. 

*Note:* SnowShu uses docker, so when you run the container make sure to add it to the snowshu network with 

```
    --network snowshu
```

## use the image

To use the image with your `replica.yml` and `credentials.yml` files in the same folder, execute like this: 

```
    docker run --network snowshu --rm -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:/workspace hutech/snowshu init 
```

*Note:* Ensure the docker socket is included as a volume. 