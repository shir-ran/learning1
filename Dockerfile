FROM eclipse-temurin:21-jre-jammy

WORKDIR /app
RUN useradd --system --uid 10001 appuser \
    && mkdir -p /app/tmp \
    && chown appuser:appuser /app/tmp

COPY --chown=appuser:appuser target/order-service-0.0.1-SNAPSHOT.jar app.jar

VOLUME ["/app/tmp"]

USER appuser
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]