.PHONY: autobahn serve

autobahn:
	docker run --rm \
		--name autobahn-server \
		-v "${PWD}/autobahn/config.json:/autobahn.json" \
		-v "${PWD}/autobahn:/reports" \
		--network host \
		crossbario/autobahn-testsuite \
		wstest -m fuzzingserver -s /autobahn.json & sleep 2

	gleam run -m autobahn; \
	docker stop autobahn-server || true

serve:
	cd "${PWD}/autobahn" && bun run serve.ts
