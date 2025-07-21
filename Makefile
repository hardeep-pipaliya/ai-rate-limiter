init-db:
	@flask db init

migrate-db:
	@flask db migrate -m "$(m)"

upgrade-db:
	@flask db upgrade

downgrade-db:
	@flask db downgrade
