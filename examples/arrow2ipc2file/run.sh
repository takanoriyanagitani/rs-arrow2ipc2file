#!/bin/sh

mkdir -p sample.d

wazero \
	run \
	-mount ./sample.d:/guest.d \
	-env ENV_OUT_IPC_NAME=/guest.d/example.output.ipc \
	./arrow2ipc2file.wasm

arrow-cat ./sample.d/example.output.ipc
