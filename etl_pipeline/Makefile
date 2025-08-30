# ETL Pipeline Docker Management

# Variables
IMAGE_NAME=whatsapp-etl
CONTAINER_NAME=whatsapp-etl-container

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run the ETL pipeline in a container
run:
	docker run --rm \
		--name $(CONTAINER_NAME) \
		-v $(PWD)/output:/app/output \
		$(IMAGE_NAME)

# Run with custom environment file
run-with-env:
	docker run --rm \
		--name $(CONTAINER_NAME) \
		-v $(PWD)/output:/app/output \
		-v $(PWD)/.env:/app/.env \
		$(IMAGE_NAME)

# Run interactively for debugging
run-interactive:
	docker run -it --rm \
		--name $(CONTAINER_NAME) \
		-v $(PWD)/output:/app/output \
		$(IMAGE_NAME) bash

# Stop running container
stop:
	docker stop $(CONTAINER_NAME)

# Remove the Docker image
clean:
	docker rmi $(IMAGE_NAME)

# Show help
help:
	@echo "Available commands:"
	@echo "  build           - Build the Docker image"
	@echo "  run             - Run the ETL pipeline"
	@echo "  run-with-env    - Run with custom environment file"
	@echo "  run-interactive - Run container with bash for debugging"
	@echo "  stop            - Stop running container"
	@echo "  clean           - Remove Docker image"
	@echo "  help            - Show this help message"
