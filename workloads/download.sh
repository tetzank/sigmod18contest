#!/bin/bash

wget -c 'http://sigmod18contest.db.in.tum.de/public.tar.gz'\
&& mkdir -p public\
&& cd public\
&& tar xf ../public.tar.gz
