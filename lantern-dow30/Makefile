# Lantern Dow 30 - Docker Management
.PHONY: help build up down restart logs test clean

# Default target
help:
	@echo "Lantern Dow 30 - Available Commands:"
	@echo "  make build     - Build the Docker containers"
	@echo "  make up        - Start all services"
	@echo "  make down      - Stop all services"  
	@echo "  make restart   - Restart all services"
	@echo "  make logs      - Show container logs"
	@echo "  make test      - Run IR discovery test"
	@echo "  make test-single TICKER=AAPL - Test single company"
	@echo "  make clean     - Clean up Docker resources"
	@echo "  make shell     - Open Airflow shell for debugging"

# Build containers
build:
	docker-compose build

# Start services
up:
	docker-compose up -d
	@echo "Airflow webserver will be available at http://localhost:8080"
	@echo "Default login: admin/admin"

# Stop services  
down:
	docker-compose down

# Restart services
restart: down up

# Show logs
logs:
	docker-compose logs -f

# Run IR discovery test
test:
	python3 test_all_dow30_ir_discovery.py

# Test single company
test-single:
	python3 -c "from lantern.discover_ir import discover_ir; from lantern.tickers import get_companies; companies = {c[0]: c for c in get_companies()}; print(f'Testing $(TICKER)...'); result = discover_ir(companies['$(TICKER)'][2], '$(TICKER)'); print(f'Result: {result}')"

# Clean Docker resources
clean:
	docker-compose down -v
	docker system prune -f

# Open Airflow shell for debugging
shell:
	docker-compose --profile debug run --rm airflow-cli bash

# Setup environment
setup:
	cp .env.template .env
	@echo "Please edit .env file with your configuration"

# Database init (if needed)
db-init:
	docker-compose run --rm airflow-webserver airflow db init

# Create admin user
create-user:
	docker-compose run --rm airflow-webserver airflow users create \
		--username admin --password admin --firstname Admin --lastname User \
		--role Admin --email admin@lantern.com