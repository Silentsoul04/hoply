prepare:
	mkdir -p data/
	cd data && wget http://files.grouplens.org/datasets/movielens/ml-20m.zip
	cd data && unzip ml-20m.zip

load:
	rm -rf data/wt
	mkdir -p data/wt
	python3 load.py
