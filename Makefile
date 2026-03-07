
check.dddmisc-core:
	uv run pytest test/bases/d3m/core

build.dddmisc-core: check.dddmisc-core
	rm -rf ./projects/dddmisc-core/dist
	uv build --wheel ./projects/dddmisc-core

publish.dddmisc-core: build.dddmisc-core
	uv publish --check-url https://pypi.org/simple/ ./projects/dddmisc-core/dist/*

check.dddmisc-messagebus:
	uv run pytest test/bases/d3m/messagebus

build.dddmisc-messagebus: check.dddmisc-messagebus
	rm -rf ./projects/dddmisc-messagebus/dist
	uv build --wheel ./projects/dddmisc-messagebus

publish.dddmisc-messagebus: build.dddmisc-messagebus
	uv publish --check-url https://pypi.org/simple/ ./projects/dddmisc-messagebus/dist/*

check.dddmisc-handlers-collection:
	uv run pytest test/bases/d3m/hc

build.dddmisc-handlers-collection: check.dddmisc-handlers-collection
	rm -rf ./projects/dddmisc-handlers-collection/dist
	uv build --wheel ./projects/dddmisc-handlers-collection

publish.dddmisc-handlers-collection: build.dddmisc-handlers-collection
	uv publish --check-url https://pypi.org/simple/ ./projects/dddmisc-handlers-collection/dist/*

check.dddmisc-domain:
	uv run pytest test/bases/d3m/domain

build.dddmisc-domain: check.dddmisc-domain
	rm -rf ./projects/dddmisc-domain/dist
	uv build --wheel ./projects/dddmisc-domain

publish.dddmisc-domain: build.dddmisc-domain
	uv publish --check-url https://pypi.org/simple/ ./projects/dddmisc-domain/dist/*

check.dddmisc-uow:
	uv run pytest test/bases/d3m/uow

build.dddmisc-uow: check.dddmisc-uow
	rm -rf ./projects/dddmisc-uow/dist
	uv build --wheel ./projects/dddmisc-uow

publish.dddmisc-uow: build.dddmisc-uow
	uv publish --check-url https://pypi.org/simple/ ./projects/dddmisc-uow/dist/*

check-all: check.dddmisc-core check.dddmisc-messagebus check.dddmisc-handlers-collection check.dddmisc-domain check.dddmisc-uow

build-all: build.dddmisc-core build.dddmisc-messagebus build.dddmisc-handlers-collection build.dddmisc-domain build.dddmisc-uow

publish-all: publish.dddmisc-core publish.dddmisc-messagebus publish.dddmisc-handlers-collection publish.dddmisc-domain publish.dddmisc-uow
