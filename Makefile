all: build run

build:
	docker build -t csv2api .
run:
	docker run -it -p 5000:5000 csv2api

stop:
	docker stop $$(docker ps -a -q --filter ancestor=csv2api --format="{{.ID}}")
remove:
	docker rmi -f csv2api

.PHONY: all build run