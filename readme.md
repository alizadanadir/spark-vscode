## To build docker image from the Dockerfile:

```bash
docker build -t spark-vscode .
```

## To run the image:

```bash
docker run -d -v ./data:/workspace -p 8443:8443 spark-vscode
```