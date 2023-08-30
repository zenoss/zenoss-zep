.PHONY: build clean

build:
	@docker run --rm \
		-v ${PWD}:/mnt/build \
		-v ${HOME}/.m2:/home/build/.m2 \
		-w /mnt/build \
		zenoss/maven:java17-1 \
		-Dprotoc.path=/usr/bin/protoc clean install

clean:
	@rm -rf core/target dist/target webapp/target