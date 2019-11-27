NAME := parallels
build:
	rm $(NAME) 2>/dev/null || :
	gcc -o $(NAME) parallels.c

install: build
	cp $(NAME) /usr/bin/$(NAME)
