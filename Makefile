main:	clean
	erlc -o ebin -I include src/*.erl

clean:
	mkdir -p ebin
	rm -f ebin/*

test:	main
	erlc -o ebin -I include test/*.erl
	erl -boot start_sasl -sname node1 -config release/erlsdb_rel.config -pz ebin -noshell -run erlsdb_test test "key" "secret"  -s init stop

VERSION=erlsdb-0.1
release:
	mkdir disttmp
	svn export `svn info . | grep '^URL:'| cut -d' ' -f2` disttmp/${VERSION}
	tar -Cdisttmp -zcvf  ${VERSION}.tar.gz ${VERSION}
	rm -rf disttmp
	echo Distribution is ${VERSION}.tar.gz
