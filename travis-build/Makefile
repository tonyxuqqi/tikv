/tmp/gflags-2.1.2/libgflags.so:
	cd /tmp && \
	curl -L https://github.com/gflags/gflags/archive/v2.1.2.tar.gz -o gflags.tar.gz && \
	tar xf gflags.tar.gz && \
	cd gflags-2.1.2 && \
	cmake . && \
	make

${ROCKSDB_DIR}/librocksdb.so:
    ${MAKE} /tmp/gflags-2.1.2/libgflags.so && \
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/tmp/gflags-2.1.2/lib" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:/tmp/gflags-2.1.2/lib" && \
	export CPLUS_INCLUDE_PATH="${CPLUS_INCLUDE_PATH}:/tmp/gflags-2.1.2/include" && \
	export CXX=${COMPILER} && \
	cd /tmp && \
	curl -L https://github.com/facebook/rocksdb/archive/rocksdb-4.3.1.tar.gz -o rocksdb.tar.gz && \
	tar xf rocksdb.tar.gz && \
	cd rocksdb-rocksdb-4.3.1 && \
	make shared_lib && \
	cp librocksdb.so* ${ROCKSDB_DIR}

${KCOV_DIR}/kcov:
	cd /tmp && \
	curl -L https://github.com/SimonKagstrom/kcov/archive/v31.tar.gz -o kcov.tar.gz && \
	tar xf kcov.tar.gz && \
	cd kcov-31 && \
	cmake . && \
	make && \
	mkdir -p ${KCOV_DIR} && \
	cp src/kcov ${KCOV_DIR}/kcov

prepare_linux: ${ROCKSDB_DIR}/librocksdb.so ${KCOV_DIR}/kcov

prepare_osx:
	brew install gflags snappy rocksdb

test_linux:
	export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	export LIBRARY_PATH="${LIBRARY_PATH}:${ROCKSDB_DIR}" && \
	ENABLE_FEATURES=default cargo test --no-run && \
	ulimit -n 2000 && \
	${KCOV_DIR}/kcov --verify --coveralls-id=${TRAVIS_JOB_ID} --include-pattern '.*/tikv/.*\.rs' target/kcov target/debug/tests-*

test_osx:
	ENABLE_FEATURES=default make test

