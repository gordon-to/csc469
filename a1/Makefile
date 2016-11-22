CFLAGS=-g -Wall

all: common.o tsc.o activity ContextSwitch

activity: activity.c common.o tsc.o
	$(CC) $< $(CC_FLAGS) common.o tsc.o -o activity

ContextSwitch: ContextSwitch.c common.o tsc.o
	$(CC) $< $(CC_FLAGS) common.o tsc.o -o ContextSwitch

common.o: common.c
	$(CC) -c $(CC_FLAGS) $< -o $@

tsc.o: tsc.c
	$(CC) -c $(CC_FLAGS) $< -o $@

clean:
	rm -f activity ContextSwitch *.o *.pyc *.eps
