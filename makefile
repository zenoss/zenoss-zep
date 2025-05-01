BUILD_IMAGE = zenoss/maven:java21-2

.DEFAULT_GOAL := build

.PHONY: build
build:
	@docker run --rm \
		-v ${PWD}:/mnt/build \
		-v ${HOME}/.m2:/home/build/.m2 \
		-w /mnt/build \
		$(BUILD_IMAGE) \
		-U clean install

.PHONY: deploy
deploy:
	@docker run --rm \
        -v ${PWD}:/mnt/build \
        -v ${HOME}/.m2:/home/build/.m2 \
        -w /mnt/build \
        $(BUILD_IMAGE) \
        -U -Drepo.login=${NEXUS_USER} -Drepo.pwd=${NEXUS_PASSWORD} clean deploy

.PHONY: build-test-img
build-test-img:
	@docker build -t zenoss/zep-test-image -f Dockerfile_test_image.in .

.PHONY: clean
clean:
	@rm -rf core/target dist/target webapp/target
