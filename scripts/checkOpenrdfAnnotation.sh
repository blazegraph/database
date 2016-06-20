#!/bin/bash

grep -l -r -i "copyright aduna" * | grep "\.java" | xargs grep -L "@openrdf"
