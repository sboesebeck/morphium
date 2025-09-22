# Morphium

Morphium ist ein Java 21+ ODM und Messaging-Framework fuer MongoDB – inklusive eigenem Wire-Protocol-Treiber, In-Memory-Testlaufzeit und einer optionalen MorphiumServer-Instanz, die sich wie MongoDB verhaelt.

## Schnellzugriff
- Uebersicht: `docs/overview.md`
- Entwicklerhandbuch: `docs/developer-guide.md`
- Messaging: `docs/messaging.md`
- How-Tos & Migration v5→v6: `docs/howtos/`
- MkDocs-Site bauen: `mkdocs serve`

## Neu in Morphium 6
- **Java 21 Pflicht**: aktualisierte Build-Pipeline, Surefire 3 und Tag-gestuetzte Testprofile.
- **Konfigurations-API**: neue Settings-Objekte (`ConnectionSettings`, `DriverSettings`, `MessagingSettings`, …) statt verteilter Setter.
- **Pluggables**: Treiber- und Messaging-Implementierungen werden ueber `@Driver` bzw. `@Messaging` entdeckt.
- **MorphiumServer & InMemoryDriver**: fast vollstaendige CRUD-, Aggregation- und Change-Stream-Paritaet zu MongoDB, inklusive `$lookup`-Pipelines, MapReduce und Resume-Token-Unterstuetzung.
- **Messaging-Architektur**: `MorphiumMessaging`-Interface mit Standard- und Advanced-Backend, verbesserter Change-Stream-Anbindung und ausgedehntem Test-Suite.
- **Neue Dokumentation**: Komplett ueberarbeitete Guides, How-Tos und Betriebshinweise.

## Anforderungen & Abhaengigkeiten
- Java 21 oder neuer
- MongoDB 5.0+ fuer produktive Deployments
- Maven

Maven-Abhaengigkeiten:
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.0.0,)</version>
</dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Migration von v5? → `docs/howtos/migration-v5-to-v6.md`

## Konfiguration & Einstieg
```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;

MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver"); // oder "InMemDriver", "SingleMongoConnectDriver"

Morphium morphium = new Morphium(cfg);
```

URI- oder Host/Port-Zugaenge lassen sich ueber Properties, Umgebungsvariablen oder System-Properties steuern:
```
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'
mvn -Dmorphium.uri='mongodb://…'
export MORPHIUM_DRIVER=inmem
```

Messaging erzeugen Sie ueber die Factory, damit Settings und Implementierung korrekt gezogen werden:
```java
var messaging = morphium.createMessaging();
messaging.start();
messaging.addListenerForTopic("user.created", (mm, msg) -> {
  // …
  return null;
});
```

## Tests & Profile
- Alle Tests: `mvn test`
- Voller Build inkl. Checks: `mvn clean verify`
- Tests ohne externe Mongo: `mvn -P inmem test`
- Pooled-/Single-Driver: `mvn -P pooled test` / `mvn -P single test`
- Externe Tests aktivieren: `mvn -P external test`
- Interaktive Wiederholungen & Log-Sammlung: `./runtests.sh` (Details: `./runtests.sh --help`)

`TestConfig` konsolidiert Einstellungen fuer Tests. Reihenfolge der Quellen:
1. System Properties (`-Dmorphium.*`)
2. Environment (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (lokale Replikat-Hosts `localhost:27017/18/19`)

## MorphiumServer & InMemoryDriver
- **InMemoryDriver**: schnelles Test-Backend ohne Mongo-Instanz, unterstuetzt Aggregation, Projektionen, Update-Resultate und Change Streams.
- **MorphiumServer**: eigenstaendiger Prozess (`java -jar morphium-6.x.jar de.caluga.morphium.server.MorphiumServer`) fuer Wire-Protocol-Clients, Tools (Compass, mongodump) und CI.
- Change Streams unterstuetzen `resumeAfter`, `startAfter`, `fullDocumentBeforeChange` und virtuelle Threads fuer leichtgewichtige Listener.

## Dokumentation & Ressourcen
- Aggregationsbeispiele: `docs/howtos/aggregation-examples.md`
- Messaging-Implementierungen: `docs/howtos/messaging-implementations.md`
- Performance- & Betriebsleitfaeden: `docs/performance-scalability-guide.md`, `docs/production-deployment-guide.md`
- Monitoring & Troubleshooting: `docs/monitoring-metrics-guide.md`, `docs/troubleshooting-guide.md`

## Mitmachen
Beitraege sind herzlich willkommen – einfach melden oder direkt Pull Requests schicken. Beachten Sie die Testprofile (`@Tag("inmemory")`, `@Tag("external")`) und halten Sie die Dokumentation aktuell.

Slack: https://join.slack.com/t/team-morphium/shared_invite/enQtMjgwODMzMzEzMTU5LTA1MjdmZmM5YTM3NjRmZTE2ZGE4NDllYTA0NTUzYjU2MzkxZTJhODlmZGQ2MThjMGY0NmRkMWE1NDE2YmQxYjI
Blog: https://caluga.de

Viel Spass!

Stephan
