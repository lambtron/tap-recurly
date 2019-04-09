
#
# Default.
#

default: build

#
# Tasks.
#

# Build.
build: 
	@pip3 install .

# Dev.
dev:
	@pip3 install -e .

# Deploy.
release:
	@python3 setup.py sdist upload

# Test.
test:
	@python3 tests/test_tap_toggl.py

# Discover.
disc: 
	@tap-recurly -c config.json --discover > catalog.json

#
# Phonies.
#

.PHONY: build
.PHONY: dev
.PHONY: release
.PHONY: schema
.PHONY: test
.PHONY: disc
