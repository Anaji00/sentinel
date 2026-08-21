FROM nginx:1.25-alpine

RUN apk add --no-cache openssl

COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /docker-entrypoint-custom.sh

RUN chmod +x /docker-entrypoint-custom.sh

EXPOSE 80 443

ENTRYPOINT ["/docker-entrypoint-custom.sh"]
