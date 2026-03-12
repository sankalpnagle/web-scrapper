docker rm rss2cl 

docker stop rss2cl 

docker run -d   --name rss2cl   -v C:\logs\rss2cl:/root/log   rss2cl

docker build -t rss2cl .


# Build image and start container
docker compose up -d --build

# Check status
docker compose ps

# View live logs (both pipelines)
docker compose logs -f

# Stop
docker compose down

# Restart after code changes
docker compose up -d --build