.PHONY: build clean

build:
	@docker run --rm \
		-v ${PWD}:/mnt/build \
		-v ${HOME}/.m2:/home/build/.m2 \
		-w /mnt/build \
		zenoss/maven:java21-2 \
		-U clean install

deploy:
	@docker run --rm \
        -v ${PWD}:/mnt/build \
        -v ${HOME}/.m2:/home/build/.m2 \
        -w /mnt/build \
        zenoss/maven:java21-2 \
        -U -Drepo.login=${NEXUS_USER} -Drepo.pwd=${NEXUS_PASSWORD} clean deploy

build-test-img:
	@docker build -t zenoss/zep-test-image -f Dockerfile_test_image.in .

clean:
	@rm -rf core/target dist/target webapp/target