# swile
head of data - swile case study


```shell
PYTHONPATH="." luigi --module pipeline CreateLoadDB --local-scheduler
docker run -d -p 12345:3000 --name metabase metabase/metabase
```
