# مثال: اگر base شما minio/minio است
FROM minio/minio:latest

# نصب curl برای دانلود mc
RUN microdnf install -y curl || true \
 && apt-get update && apt-get install -y curl || true \
 && apk add --no-cache curl || true

# دانلود mc
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc \
 && chmod +x /usr/local/bin/mc

# بقیه تنظیمات خودت...
