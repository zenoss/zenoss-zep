BUILD_IMAGE = zenoss/maven:java21-2

# Artifact Registry Maven repository require credentials.
GOOGLE_APPLICATION_CREDENTIALS ?= $(HOME)/.config/gcloud/application_default_credentials.json
DOCKER_RUN_ARGS = -v $(GOOGLE_APPLICATION_CREDENTIALS):/home/build/google_application_credentials.json \
                   -e GOOGLE_APPLICATION_CREDENTIALS=/home/build/google_application_credentials.json


.DEFAULT_GOAL := build

.PHONY: build
build:
	@docker run --rm $(DOCKER_RUN_ARGS) \
		-v ${PWD}:/mnt/build \
		-v ${HOME}/.m2/repository:/home/build/.m2/repository \
		-w /mnt/build \
		$(BUILD_IMAGE) \
		--batch-mode --no-transfer-progress -U clean install

.PHONY: deploy
deploy:
	@docker run --rm $(DOCKER_RUN_ARGS) \
        -v ${PWD}:/mnt/build \
        -v ${HOME}/.m2/repository:/home/build/.m2/repository \
        -w /mnt/build \
        $(BUILD_IMAGE) \
        -U clean deploy

.PHONY: build-test-img
build-test-img:
	@docker build -t zenoss/zep-test-image -f Dockerfile_test_image.in .

.PHONY: clean
clean:
	@rm -rf core/target dist/target webapp/target
