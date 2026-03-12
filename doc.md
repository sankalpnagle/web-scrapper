docker rm rss2cl 

docker stop rss2cl 

docker run -d   --name rss2cl   -v C:\logs\rss2cl:/root/log   rss2cl

docker build -t rss2cl .