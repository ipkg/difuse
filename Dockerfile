
FROM alpine

ADD ./difused-linux /difused

EXPOSE 4624
EXPOSE 9090

CMD [ "/difused", "-b", "0.0.0.0:4624", "-a", "0.0.0.0:9090" ]
