all:
	@echo "try make load"

clean:
	rm -rf db

download: data
	wget http://conceptnet5.media.mit.edu/downloads/current/conceptnet5_db_5.4.tar.bz2

unpack: download
	tar xvf conceptnet5_db_5.4.tar.bz2
	rm conceptnet5_db_5.4.tar.bz2

load: unpack
	python load.py

check:
	py.test --cov=. --cov-report=html tests.py

