# Build file for CS61C Project 1  [Spring 2011]
# You should not need to edit this file if you're working on the inst machines.

# This file requires GNU make and depends on paths on instruction machines.

# If you are working on your own machine, you will need to edit the paths.


####

## Variables

# Source files (java code). wildcard selects all files matching a pattern.
SOURCES = $(wildcard *.java)
# Output JAR file
TARGET = proj1.jar
# Extra JARs to have on the classpath when compiling.
CLASSPATH = /home/ff/cs61c/hadoop/hadoop-core.jar:/home/ff/cs61c/hadoop/lib/commons-cli.jar
# javac command to use
JAVAC = javac -g -deprecation -cp $(CLASSPATH)
# jar command to use
JAR = jar

## Make targets

# General form is target: dependencies (targets or files), followed by
# commands to run to build the target from the dependencies.

# Default target.
all: $(TARGET)

$(TARGET): classes $(SOURCES)
	$(JAVAC) -d classes $(SOURCES)
	$(JAR) cf $(TARGET) -C classes .

classes:
	mkdir classes

clean:
	rm -rf classes $(TARGET) *~

.PHONY: clean all
