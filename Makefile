
check.dddmisc-core:
	poetry check-project -C ./projects/dddmisc-core
	poetry run pytest test/bases/d3m/core

build.dddmisc-core: check.dddmisc-core
	rm -rf ./projects/dddmisc-core/dist
	poetry build-project -C ./projects/dddmisc-core

publish.dddmisc-core: build.dddmisc-core
	poetry publish --skip-existing -C ./projects/dddmisc-core

check.dddmisc-messagebus:
	poetry check-project -C ./projects/dddmisc-messagebus
	poetry run pytest test/bases/d3m/messagebus

build.dddmisc-messagebus: check.dddmisc-messagebus
	rm -rf ./projects/dddmisc-messagebus/dist
	poetry build-project -C ./projects/dddmisc-messagebus

publish.dddmisc-messagebus: build.dddmisc-messagebus
	poetry publish --skip-existing -C ./projects/dddmisc-messagebus

check.dddmisc-handlers-collection:
	poetry check-project -C ./projects/dddmisc-handlers-collection
	poetry run pytest test/bases/d3m/hc

build.dddmisc-handlers-collection: check.dddmisc-handlers-collection
	rm -rf ./projects/dddmisc-handlers-collection/dist
	poetry build-project -C ./projects/dddmisc-handlers-collection

publish.dddmisc-handlers-collection: build.dddmisc-handlers-collection
	poetry publish --skip-existing -C ./projects/dddmisc-handlers-collection

check.dddmisc-domain:
	poetry check-project -C ./projects/dddmisc-domain
	poetry run pytest test/bases/d3m/domain

build.dddmisc-domain: check.dddmisc-domain
	rm -rf ./projects/dddmisc-domain/dist
	poetry build-project -C ./projects/dddmisc-domain

publish.dddmisc-domain: build.dddmisc-domain
	poetry publish --skip-existing -C ./projects/dddmisc-domain

check.dddmisc-uow:
	poetry check-project -C ./projects/dddmisc-uow
	poetry run pytest test/bases/d3m/uow

build.dddmisc-uow: check.dddmisc-uow
	rm -rf ./projects/dddmisc-uow/dist
	poetry build-project -C ./projects/dddmisc-uow

publish.dddmisc-uow: build.dddmisc-uow
	poetry publish --skip-existing -C ./projects/dddmisc-uow

check-all: check.dddmisc-core check.dddmisc-messagebus check.dddmisc-handlers-collection check.dddmisc-domain check.dddmisc-uow

build-all: build.dddmisc-core build.dddmisc-messagebus build.dddmisc-handlers-collection build.dddmisc-domain build.dddmisc-uow

publish-all: publish.dddmisc-core publish.dddmisc-messagebus publish.dddmisc-handlers-collection publish.dddmisc-domain publish.dddmisc-uow
