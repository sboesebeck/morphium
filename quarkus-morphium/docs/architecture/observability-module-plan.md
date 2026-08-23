# Architektur-Plan: Optionales Observability-Modul für `quarkus-morphium`

> Autor: datona-architect (Agent-Entwurf)
> Status: **Vorschlag / zur Diskussion** — noch nicht implementiert
> Repo: `morphium` (Maven-Modul `quarkus-morphium/`)
> Letzte Prüfung des Codes: 2026-08-23

---

## 1. Ziel & Auslöser

**Auslöser:** Im Repo `datona-version42-adapter-ng-workspace/main-quarkus` wurde beobachtet,
dass Teams, die `quarkus-morphium` UND eine Micrometer/OpenTelemetry-Metrics-Extension
(`quarkus-micrometer`, `quarkus-micrometer-registry-prometheus`, `quarkus-opentelemetry`, …)
gleichzeitig einsetzen, händisch Boilerplate schreiben, um MongoDB-Operationsmetriken
(Connection-Pool, Latenzen, Fehlerzähler) in ihr Metrics-Backend zu spiegeln.

**Ziel dieses Plans:** Ein neues, **rein optionales** Sub-Modul der `quarkus-morphium`-Extension,
das — nur wenn eine Metrics-Extension bereits auf dem Klassenpfad der Anwendung liegt —
automatisch Morphium/MongoDB-Kennzahlen als Micrometer-`Meter`s registriert. Apps ohne
Metrics-Extension dürfen **keine** zusätzliche Dependency, keinen zusätzlichen Klassenpfad-Eintrag
und keine Verhaltensänderung bekommen.

---

## 2. Verifizierter Code-Kontext (Ist-Zustand)

### 2.1 Modulstruktur (bestehend)
```
quarkus-morphium/
├── runtime/        (quarkus-morphium)            – CDI-Producer, Config, Interceptors
├── deployment/      (quarkus-morphium-deployment) – BuildSteps, Jandex-Scanning, Capabilities-Gates
├── testing/
└── integration-tests/
```
Parent-POM (`quarkus-morphium/pom.xml`) verwaltet die Quarkus-BOM; jedes Sub-Modul hat eine
eigene `pom.xml` mit `parent = quarkus-morphium-parent`.

### 2.2 Bereits etabliertes Muster für "optionale Integration bei vorhandener Capability"

`MorphiumProcessor.registerMorphiumIdJsonCustomizers(...)` (deployment) ist das **exakte
Vorbild** für das, was wir bauen wollen:

```java
@BuildStep
void registerMorphiumIdJsonCustomizers(Capabilities capabilities,
                                       BuildProducer<AdditionalBeanBuildItem> additionalBeans) {
    if (capabilities.isPresent(Capability.JACKSON)) {
        additionalBeans.produce(AdditionalBeanBuildItem.builder()
            .addBeanClass("de.caluga.morphium.quarkus.json.MorphiumIdJacksonModule")
            .setUnremovable().build());
    }
    if (capabilities.isPresent(Capability.JSONB)) { ... }
}
```
Kombiniert mit optionalen Maven-Dependencies (`<optional>true</optional>` auf
`quarkus-jackson` / `quarkus-jsonb` in `runtime/pom.xml`, plus die `-deployment`-Pendants in
`deployment/pom.xml`) und Deployment-Parity (Quarkus verlangt: jede optionale Runtime-Dependency
braucht ein `-deployment`-Gegenstück im Deployment-Modul, sonst schlägt der
Extension-Consistency-Check fehl).

**→ Dieses Muster wird 1:1 für Micrometer/OpenTelemetry wiederverwendet.**

### 2.3 Bereits vorhandene Kennzahlen-Quellen in Morphium-Core (kein neuer Code für Datenerhebung nötig)

| Quelle | Typ | Inhalt |
|---|---|---|
| `MorphiumDriver.getDriverStats()` → `Map<DriverStatsKey, Double>` | Pull (on-demand) | `CONNECTIONS_OPENED/CLOSED/BORROWED/RELEASED/IN_POOL/IN_USE`, `ERRORS`, `FAILOVERS`, `MSG_SENT`, `REPLY_*`, `THREADS_CREATED`, `THREADS_WAITING_FOR_CONNECTION` |
| `Morphium.getStatistics()` → `Statistics extends HashMap<String,Double>` | Pull (on-demand) | `StatisticKeys`: `WRITES`, `WRITES_CACHED`, `READS`, `CHITS`, `CMISS`, `NO_CACHED_READS`, `CHITSPERC`, `CMISSPERC`, `CACHE_ENTRIES`, `REGISTERED_LOGGERS`, `WRITE_BUFFER_ENTRIES`, `PULL`, `PULLSKIP`, `SKIPPED_MSG_UPDATES`, `INSTANCE_COUNT` |
| `driver.getNumConnectionsByHost()` | Pull | Connections pro Host (für Multi-Host-Setups) |
| `MorphiumStorageListener<T>` (Interface, bereits für `MorphiumBlockingCallDetector` genutzt) | Push (Event) | `preStore`/`postStore`/`preRemove`/`postRemove`/`postLoad`/`preUpdate`/`postUpdate` — je Aufruf, mit Objekt/Query/Class-Kontext |
| `MorphiumTransactionEvent` (CDI Event, `@MorphiumTxPhase`) | Push (CDI Event) | `BEFORE_COMMIT`/`AFTER_COMMIT`/`AFTER_ROLLBACK`, inkl. Exception bei Rollback |
| `MorphiumReadinessCheck` | Pull (schon gebaut) | liest bereits `getDriverStats()` als Health-Metadata — Beleg, dass die Werte zur Laufzeit zugreifbar sind |

**Wichtiger Befund:** Es gibt **keine** dedizierte "MetricsListener"/"ProfilingListener"-Klasse
in `morphium-core` — die Statistik-APIs sind reine Pull-Snapshots (`Map<String,Double>`), die
Event-Hooks (`MorphiumStorageListener`, `MorphiumTransactionEvent`) sind die einzigen
Push-Mechanismen. Das Observability-Modul muss beide Muster kombinieren:
- **Gauges** aus periodischem Pull der Statistics/DriverStats-Maps.
- **Counter/Timer** aus dem Push-Pfad (`MorphiumStorageListener`, Transaction-Events) für
  Latenz- und Fehlerzähler pro Operation.

### 2.4 CDI-Lifecycle-Realität, die das Design einschränkt

- `Morphium` ist ein **normal-scoped, lazy** CDI-Bean (`MorphiumProducer.morphium()` mit
  double-checked locking) — der erste Proxy-Zugriff löst den echten Connect aus. Ein
  Observability-Feature darf **niemals** `Instance<Morphium>.get()` aufrufen, um sich früh zu
  registrieren (exakt das Problem, das `MorphiumBlockingCallDetector`s Javadoc dokumentiert:
  ein früherer `@Observes StartupEvent`-Ansatz löste ungewollt den Connect aus). Die Registrierung
  muss **innerhalb** von `MorphiumProducer.buildMorphium()`, nach dem echten Connect, erfolgen —
  wie `MorphiumBlockingCallDetector.registerOn(instance)` es bereits tut (Zeile ~497 in
  `MorphiumProducer.java`, direkt vor dem `log.info(...)` Banner).
- Native-Image-Kompatibilität: keine Reflection außerhalb der bereits etablierten
  `ClassGraphCache.preRegister*`-Mechanik; keine neuen `sun.*`/`Unsafe`-Zugriffe.
- Dev-Mode Hot-Reload: `instance` wird bei jedem Hot-Reload neu gebaut (`buildMorphium()` läuft
  erneut) — jede Meter-Registrierung muss **idempotent** sein (Micrometer wirft bei
  Doppel-Registrierung mit identischem `Id` keinen Fehler, aber doppelte `MeterBinder`-Aufrufe
  ohne Deduplizierung erzeugen doppelte Callback-Referenzen auf alte `Morphium`-Instanzen ⇒
  Memory-Leak / falsche Werte nach Reload). Siehe Abschnitt 6.4.

### 2.5 Was zu Micrometer/OpenTelemetry bereits im Repo existiert

Keine Treffer für `Capability.METRICS`, `MICROMETER` oder `OPENTELEMETRY` im gesamten
`morphium-workspace` — die Extension hat **aktuell keine Berührung** mit Metrics/Tracing.
Das neue Modul ist somit komplett grüne Wiese, aber mit einem klaren Vorbild (JSON-Customizer-Muster).

---

## 3. Architekturentscheidung: Neues Sub-Modul vs. In-Place-Erweiterung

### Optionen

| Option | Beschreibung | Bewertung |
|---|---|---|
| **A. Neues Maven-Sub-Modul** `quarkus-morphium-observability` (+ `-deployment`) | Eigenständige Extension, die `quarkus-morphium` UND `quarkus-micrometer` als **beide optional/required** deklariert | ✅ Saubere Trennung, kein Zwang für Micrometer-Dependency in der Haupt-Extension; **aber**: zweite Extension zum Pflegen, zweiter Versionsstand, Nutzer müssen zwei Coordinates kennen |
| **B. Klassen direkt in `quarkus-morphium` (runtime+deployment), Micrometer als `<optional>true</optional>`** | Wie das bestehende Jackson/JSON-B-Muster (Abschnitt 2.2), nur für Micrometer | ✅ Ein Artefakt, ein Versionsstand, Nutzer bekommen Observability "geschenkt" sobald sie Micrometer schon haben; folgt 1:1 etabliertem Repo-Muster; ⚠️ etwas mehr Verantwortung im Kern-Modul |
| **C. Separates Community-Extension-Projekt außerhalb des Morphium-Reactors** | Wie es früher `io.quarkiverse.morphium` war (siehe README "Migrating from standalone") | ❌ Widerspricht der bewussten Entscheidung, alles in den Morphium-Reactor zu holen (README: "this extension is now an optional module... built in lockstep") |

### Empfehlung: **Option B** — Erweiterung von `quarkus-morphium` (runtime + deployment), kein neues Top-Level-Modul

**Begründung:**
1. Das Repo hat mit dem Jackson/JSON-B-Muster bereits bewiesen, dass "optional dependency +
   Capabilities-Gate im selben Modul" der etablierte, von den Maintainern akzeptierte Weg ist —
   ein neues Sub-Modul für exakt dasselbe Muster wäre unnötige Divergenz.
2. Ein zusätzliches Maven-Modul bedeutet: eigene `pom.xml`, eigener Eintrag in
   `quarkus-morphium/pom.xml` `<modules>`, eigene Extension-Metadata
   (`quarkus-extension.yaml` via `quarkus-extension-maven-plugin`), eigene Versionierung im
   Reactor, eigene Release-Koordination — Mehraufwand ohne technischen Zwang, da Capabilities-Gates
   bereits verhindern, dass Nicht-Metrics-Apps etwas davon spüren.
3. **Einzige Ausnahme, die ein eigenes Modul rechtfertigen würde:** falls das Observability-Modul
   selbst harte (non-optional) Compile-Abhängigkeiten zu Micrometer-APIs bräuchte, die den
   Bytecode von `quarkus-morphium` aufblähen, auch wenn die Capability nicht aktiv ist. Das ist
   vermeidbar (siehe Abschnitt 5) — Micrometer-Typen werden ausschließlich in isolierten, über
   `Capabilities.isPresent(...)` gated Klassen referenziert, exakt wie
   `MorphiumIdJacksonModule`/`MorphiumIdJsonbModule` heute mit Jackson/JSON-B.

**Falls das Team dennoch strikte Modul-Trennung will** (z. B. weil Observability-Code schneller
iterieren soll als der Core), ist Option A die Fallback-Wahl — der Rest dieses Plans (BuildSteps,
Capabilities-Gate-Logik, Metrik-Katalog, Naming) bleibt inhaltlich identisch und wird nur auf
zwei Module statt zwei Package innerhalb eines Moduls verteilt.

---

## 4. Zielarchitektur (Option B im Detail)

### 4.1 Neue Klassen (runtime, Package `de.caluga.morphium.quarkus.observability`)

| Klasse | Verantwortung |
|---|---|
| `MorphiumMetricsBinder` | Registriert Micrometer-`Gauge`s für Driver-Stats (`getDriverStats()`) und Morphium-Statistics (`getStatistics()`) gegen eine injizierte `MeterRegistry`. Analog zu bestehenden Micrometer-Bindern (`io.micrometer.core.instrument.binder.MeterBinder`). |
| `MorphiumMetricsStorageListener implements MorphiumStorageListener<Object>` | Zählt/misst Store/Remove/Load/Update-Operationen als Micrometer `Counter`/`Timer` (Tags: `operation`, `entity` = Klassenname, `outcome` = `success`/`error` via `MorphiumAccessVetoException`). Wird analog zu `MorphiumBlockingCallDetector.registerOn(...)` direkt in `buildMorphium()` registriert. |
| `MorphiumMetricsTransactionObserver` | CDI-Observer auf `@MorphiumTxPhase(...)`-Events; zählt Commits/Rollbacks/CosmosDB-Degradierungen. |
| `MorphiumObservabilityRuntimeConfig` (`@ConfigMapping(prefix="quarkus.morphium.observability")`) | Feature-Flags: `enabled` (Default `true`, aber nur wirksam wenn Capability vorhanden), `poll-interval` für die Gauge-Refresh-Strategie (falls Pull statt Push), `include-tags` (z. B. Host-Tags optional wegen Tag-Kardinalität, siehe 6.3). |

### 4.2 Neue Klassen (deployment)

| Klasse/Methode | Verantwortung |
|---|---|
| `MorphiumProcessor.registerObservability(Capabilities, BuildProducer<AdditionalBeanBuildItem>)` (neue `@BuildStep`-Methode, gleiche Klasse wie bestehendes JSON-Gate) | Registriert `MorphiumMetricsBinder`, `MorphiumMetricsStorageListener`, `MorphiumMetricsTransactionObserver` als `AdditionalBeanBuildItem` **nur wenn** `capabilities.isPresent(Capability.METRICS)` (Micrometer) **oder** eine äquivalente OTel-Metrics-Capability vorhanden ist (siehe 4.4 zur Capability-Wahl). |
| ggf. `MorphiumObservabilityBuildTimeConfig` (`@ConfigRoot(phase=BUILD_TIME)`) | Falls ein Build-Time-Kill-Switch gewünscht ist (analog `MorphiumHealthBuildTimeConfig`), um das Feature komplett aus dem nativen Image auszuschließen, selbst wenn Micrometer zufällig transitiv vorhanden ist. |

### 4.3 Maven-Dependency-Schnitt (analog Jackson/JSON-B-Muster)

`runtime/pom.xml` (Ergänzung):
```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-micrometer</artifactId>
  <optional>true</optional>
</dependency>
```
`deployment/pom.xml` (Ergänzung):
```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-micrometer-deployment</artifactId>
  <optional>true</optional>
</dependency>
```
Keine Abhängigkeit zu einem konkreten Registry-Backend (Prometheus/OTLP/…) — Micrometer selbst
ist Backend-agnostisch; die App entscheidet über ihre eigene `quarkus-micrometer-registry-*`-Wahl.

### 4.4 Capability-Wahl: Micrometer vs. OpenTelemetry Metrics

Quarkus kennt (Stand Quarkus 3.x, wie in `quarkus-bom` dieses Reactors verwendet) die
Capability `io.quarkus.deployment.Capability.METRICS`, die sowohl von
`quarkus-micrometer` als auch — sofern die App die Micrometer-OTel-Bridge nutzt — indirekt
gesetzt wird. **Reines** `quarkus-opentelemetry` (Tracing) ohne Micrometer-Bridge setzt
`Capability.OPENTELEMETRY_TRACER`, nicht `METRICS`.

**Entscheidung:** Dieses Modul bindet sich an die **Micrometer-API** (`MeterRegistry`,
`Gauge`, `Counter`, `Timer`) und gated auf `Capability.METRICS`. Das ist die richtige Wahl, weil:
- Micrometer ist der De-facto-Metrics-Standard in Quarkus; `quarkus-opentelemetry` kann seine
  Metrik-Exportpfade selbst über eine Micrometer→OTel-Bridge (`quarkus-micrometer-registry-otlp`)
  laufen lassen — wir müssen keine zweite native OTel-Metrics-API direkt bedienen.
- Reine Tracing-only-Apps (nur `quarkus-opentelemetry`, kein Micrometer) bekommen dieses Feature
  bewusst **nicht** — sie haben keinen Metrics-Sink, an den wir etwas senden könnten. Das ist
  korrekt und kein Gap; ein zukünftiges Tracing-Modul (Spans um MongoDB-Operationen) wäre ein
  **separates** Feature mit eigener Capability-Prüfung (`Capability.OPENTELEMETRY_TRACER`),
  nicht Teil dieses Metrics-Plans (siehe Abschnitt 8, "Out of Scope").
- Diese Namensgebung muss vor der Implementierung gegen den tatsächlich im Reactor gepinnten
  Quarkus-BOM-Stand verifiziert werden (`grep Capability.METRICS` im
  `io.quarkus:quarkus-core-deployment`-JAR der exakten `${quarkus.version}`), da sich exakte
  Capability-Konstanten zwischen Quarkus-Minor-Versionen verschieben können — dieser Plan
  spezifiziert das Verhalten, nicht die exakte Konstante; das ist ein Implementierungsdetail,
  das beim Anlegen des ersten Patches zu verifizieren ist (in dieser Analyse nicht mit
  Tool-Zugriff auf die JARs verifizierbar gewesen, s. Abschnitt 9 „Offene Verifikationen").

---

## 5. Metrik-Katalog (Vorschlag)

Alle Metrik-Namen folgen Micrometer-Konvention (`snake_case`, Einheit als Suffix wo sinnvoll)
und dem Präfix `morphium.*` (analog zu `mongodb.driver.*` bei anderen Quarkus-DB-Extensions).

| Metrikname | Typ | Quelle | Tags | Beschreibung |
|---|---|---|---|---|
| `morphium.driver.connections.pool` | Gauge | `DriverStatsKey.CONNECTIONS_IN_POOL` | `database` | Verbindungen im Pool |
| `morphium.driver.connections.in_use` | Gauge | `CONNECTIONS_IN_USE` | `database` | Aktiv genutzte Verbindungen |
| `morphium.driver.connections.borrowed` | Counter (aus Gauge-Delta oder direkt kumulativ, da Driver bereits kumulativ zählt) | `CONNECTIONS_BORROWED` | `database` | Kumulative Anzahl ausgeliehener Verbindungen |
| `morphium.driver.connections.released` | Counter | `CONNECTIONS_RELEASED` | `database` | Kumulative Rückgaben |
| `morphium.driver.threads.waiting` | Gauge | `THREADS_WAITING_FOR_CONNECTION` | `database` | Wartende Threads auf Connection (Pool-Sättigung-Signal) |
| `morphium.driver.errors` | Counter | `ERRORS` | `database` | Treiberfehler kumulativ |
| `morphium.driver.failovers` | Counter | `FAILOVERS` | `database` | Replica-Set-Failover-Ereignisse |
| `morphium.cache.hit_ratio` | Gauge | `CHITSPERC` | `database` | Cache-Trefferquote (%) |
| `morphium.cache.entries` | Gauge | `CACHE_ENTRIES` | `database` | Aktuelle Cache-Einträge |
| `morphium.write_buffer.entries` | Gauge | `WRITE_BUFFER_ENTRIES` | `database` | Ausstehende gepufferte Writes |
| `morphium.operations.duration` | Timer | `MorphiumStorageListener` (pre/post-Paare) | `operation` (`store`/`remove`/`update`/`load`), `entity`, `outcome` | Latenz je Operationstyp |
| `morphium.operations.errors` | Counter | `MorphiumStorageListener` (`MorphiumAccessVetoException` in pre-Hooks) | `operation`, `entity` | Von einem `@PreStore`/`@PreRemove`-Listener abgelehnte Operationen |
| `morphium.transactions.commits` | Counter | `MorphiumTransactionEvent(AFTER_COMMIT)` | — | Erfolgreiche Commits |
| `morphium.transactions.rollbacks` | Counter | `MorphiumTransactionEvent(AFTER_ROLLBACK)` | `reason` (Exception-Klassenname, niedrige Kardinalität durch Whitelist) | Rollbacks |
| `morphium.transactions.cosmosdb_degraded` | Counter | `MorphiumTransactionalInterceptor.isCosmosDb()`-Pfad | — | Wie oft die Transaktions-Wrapper-Degradierung für CosmosDB gegriffen hat |

**Wichtig — Kardinalitätsrisiko:** `driver.getNumConnectionsByHost()` liefert einen Wert **pro
Host** (siehe `MorphiumReadinessCheck`, das dies bereits als `host:<hostname>`-Health-Metadata
tut). Als Micrometer-Tag `host=<hostname>` wäre das bei dynamischen/vielen Hosts
(Kubernetes-Pods, Atlas-Sharding) ein Tag-Explosion-Risiko. **Entscheidung:** Diese
Pro-Host-Aufschlüsselung wird **nicht** standardmäßig als Metrik exportiert; nur die
aggregierten `DriverStatsKey`-Werte. Eine Opt-in-Property
(`quarkus.morphium.observability.per-host-connections=false` Default) kann das für kleine,
statische Cluster nachrüsten — als klar dokumentiertes Kardinalitätsrisiko, nicht als Default.

---

## 6. Kritische Entwurfsfragen & Antworten

### 6.1 Wo wird registriert, ohne den Lazy-Connect zu triggern?

Registrierung **ausschließlich** am Ende von `MorphiumProducer.buildMorphium()`, direkt neben
der bestehenden Zeile (aktuell ca. `MorphiumBlockingCallDetector.registerOn(instance)`
fehlt noch im aktuell gelesenen Ausschnitt bis Zeile 500 — muss beim Patch verifiziert werden,
aber der Javadoc von `MorphiumBlockingCallDetector` belegt exakt diesen Aufrufort/diese Reihenfolge).
Ein `MeterRegistry`-Bean wird **nur dann** per `@Inject Instance<MeterRegistry>` referenziert,
wenn Capabilities.METRICS zur Build-Zeit als vorhanden erkannt wurde — sonst wird die gesamte
Binder-Klasse gar nicht als CDI-Bean registriert (das ist der Kern des Capabilities-Gates,
nicht ein Runtime-`if`).

### 6.2 Pull (Gauge) vs. Push (Counter/Timer) — warum beides?

- **Gauges** für Zustände, die keinen sinnvollen "Ereignis"-Charakter haben (Pool-Größe,
  Cache-Füllstand) — Micrometer `Gauge.builder(name, statsSupplier, extractor)` mit einer
  `WeakReference` auf die `Morphium`-Instanz (Standard-Micrometer-Pattern, verhindert
  GC-Leaks bei Hot-Reload/Shutdown).
- **Counter/Timer** für Ereignisse mit klarer Semantik (eine Operation passiert, dauert X ms,
  endet in Erfolg/Fehler) — nur über die Event-Hooks (`MorphiumStorageListener`,
  `MorphiumTransactionEvent`) sauber abbildbar, da die Pull-Statistics-Maps keine
  Latenz-Verteilung liefern (nur kumulative Zähler, keine Timer/Histogramme).

### 6.3 Muss Morphium-Core geändert werden?

**Nein, im MVP nicht zwingend.** Alle in Abschnitt 5 genannten Metriken sind aus bereits
öffentlichen APIs ableitbar (`getDriverStats()`, `getStatistics()`, `MorphiumStorageListener`,
`MorphiumTransactionEvent`). Eine mögliche **spätere** Erweiterung (Out of Scope für dieses
Modul, aber als Anschlusspunkt zu vermerken): `MorphiumStorageListener` liefert aktuell keine
Latenz direkt — der Timer muss im Quarkus-Modul selbst über
`preStore()`-Zeitstempel/`ThreadLocal` + `postStore()`-Differenz gebaut werden, da die
Store-Operation selbst zwischen pre/post im Aufrufer (`Morphium.store()`) liegt, nicht im
Listener. Das ist machbar, aber pro Thread korrekt zu synchronisieren (Reentrancy bei
verschachtelten Store-Aufrufen beachten — z. B. `@PreStore`-Callback, der selbst ein anderes
Objekt speichert).

### 6.4 Idempotenz bei Dev-Mode Hot-Reload

Da `buildMorphium()` bei jedem Hot-Reload erneut läuft und eine **neue** `Morphium`-Instanz
erzeugt, muss die Registrierung:
1. Alte Meter-Bindings der vorherigen Instanz explizit deregistrieren (`MeterRegistry.remove(Meter)`)
   bevor neue registriert werden — sonst zeigt z. B. `morphium.driver.connections.pool` nach
   drei Hot-Reloads drei überlagerte Gauges mit stale `WeakReference`s.
2. Dies spiegelt exakt das Problem, das `MorphiumProducer.onStop()` bereits für die
   `Morphium`-Instanz selbst löst (`instance = null` im Shutdown-Observer) — die
   Metrik-Registrierung braucht ein äquivalentes Gegenstück, ausgelöst entweder im selben
   `onStop()`-Pfad oder in einem eigenen `@PreDestroy`-Hook auf dem Binder-Bean.
3. **Empfehlung:** `MorphiumMetricsBinder` hält selbst die Liste der von ihm registrierten
   `Meter`-Handles und entfernt sie in einer `close()`-Methode, die von `MorphiumProducer.onStop()`
   zusätzlich zum bestehenden `instance.close()` aufgerufen wird (Producer braucht dafür eine
   `Instance<MorphiumMetricsBinder>`-Referenz, ebenfalls nur injiziert wenn die Bean existiert).

### 6.5 Native-Image-Verträglichkeit

Micrometer-Core ist selbst GraalVM-native-fähig (Quarkus' eigene `quarkus-micrometer`-Extension
bringt bereits die nötigen `RuntimeReflectionRegistration`/Substitutions mit). Dieses Modul
fügt **keine neue Reflection** hinzu — alle Aufrufe (`MeterRegistry.gauge(...)`,
`Counter.builder(...).register(...)`) sind normale Methodenaufrufe. Einzige Prüfpflicht: keine
Lambda-Referenz auf `Morphium`-Instanzen darf eine starke Referenz sein, die den nativen
Image-Heap unnötig aufbläht (Gauges mit `WeakReference`, siehe 6.2/6.4).

### 6.6 Health-Check-Überlappung

`MorphiumReadinessCheck` liest bereits `getDriverStats()` als Health-Metadata (informativ, kein
Einfluss auf UP/DOWN). Das ist **keine Redundanz, sondern zwei verschiedene Konsumenten
derselben Datenquelle** (Health-Probe = Momentaufnahme für Orchestrator; Metrics = Zeitserie für
Monitoring/Alerting) — kein Konflikt, kein Refactoring von `MorphiumReadinessCheck` nötig.

---

## 7. Konfigurationsschnitt (`quarkus.morphium.observability.*`)

| Property | Default | Bedeutung |
|---|---|---|
| `quarkus.morphium.observability.enabled` | `true` | Feature-Flag; wirkt nur wenn Micrometer-Capability zur Build-Zeit erkannt wurde. Erlaubt Nutzern mit Micrometer-Dependency (z. B. transitiv über ein anderes Feature), das Feature trotzdem abzuschalten. |
| `quarkus.morphium.observability.poll-interval` | `10s` | Intervall für Gauge-Refresh, falls kein reiner On-Demand-Callback verwendet wird (Micrometer-`Gauge` ruft den Supplier bei jedem Scrape ab — bei Prometheus-Pull-Modell meist kein separates Polling nötig; Property als Reserve für Push-Backends wie OTLP mit periodischem Export). |
| `quarkus.morphium.observability.per-host-connections` | `false` | Siehe Kardinalitätswarnung Abschnitt 5. |
| `quarkus.morphium.observability.include-storage-listener-metrics` | `true` | Erlaubt Deaktivierung der Store/Remove/Load-Counter/Timer separat von den reinen Pool-Gauges (z. B. bei sehr hohem Operationsvolumen, wo Timer-Overhead spürbar wird). |

Ein Build-Time-Root (`MorphiumObservabilityBuildTimeConfig`, analog
`MorphiumHealthBuildTimeConfig`) für einen harten Kill-Switch, der die Beans schon zur Bauzeit
nicht registriert (statt nur zur Laufzeit zu deaktivieren), ist empfehlenswert für native
Images, wo jedes vermiedene Bean Startzeit/Image-Größe spart.

---

## 8. Out of Scope (bewusst nicht Teil dieses Plans)

- **Distributed Tracing / Spans** um einzelne MongoDB-Operationen (`Capability.OPENTELEMETRY_TRACER`)
  — eigenständiges, späteres Feature mit eigener Span-Namenskonvention
  (`db.system=mongodb`-Semantic-Conventions), nicht Teil des Metrics-Katalogs hier.
- **Änderungen an `morphium-core`** zur Bereitstellung neuer Roh-Metriken (z. B. Latenz-Histogramme
  direkt im Driver) — MVP kommt ohne aus (Abschnitt 6.3); als Folgearbeit vermerkt, falls sich
  Timer-Aufbau im Quarkus-Modul als zu ungenau/fehleranfällig erweist.
- **Konkrete Registry-Backends** (Prometheus-Endpoint, OTLP-Exporter-Konfiguration) — das ist
  Sache von `quarkus-micrometer-registry-*`, nicht dieses Moduls.
- **Dev-UI-Integration** (Live-Metrik-Anzeige im `/q/dev-ui/`) — denkbare spätere Ergänzung
  analog zum bestehenden `MorphiumDevUIProcessor`, aber nicht Teil des MVP.

---

## 9. Offene Verifikationen vor Implementierungsstart

1. ~~**Exakte Capability-Konstante** für Micrometer in der im Reactor gepinnten
   `quarkus.version` verifizieren~~ — **ERLEDIGT (23.08.2026):** Reactor pinnt `quarkus.version`
   `3.32.3` (`pom.xml:118`). Direkt gegen `quarkus-core-deployment-3.32.3.jar` geprüft
   (`unzip -p ... io/quarkus/deployment/Capability.class | javap -`): `Capability.METRICS`,
   `Capability.OPENTELEMETRY_TRACER`, `Capability.OPENTELEMETRY_METRICS` existieren alle exakt so.
   **Zu verwenden: `Capability.METRICS`** (Begründung Abschnitt 4.4 bleibt gültig).
2. **Extension-Parity-Check** (Quarkus' eigener Build-Schritt, der prüft, dass jede optionale
   Runtime-Dependency ein `-deployment`-Gegenstück hat) lokal mit
   `mvn -pl quarkus-morphium/runtime,quarkus-morphium/deployment -am verify` gegen die neue
   `quarkus-micrometer`/`quarkus-micrometer-deployment`-Optional-Dependency laufen lassen.
3. **Tag-Kardinalität in der Praxis** — mit dem `version42-adapter`-Team abstimmen, ob
   `per-host-connections=false` als Default für deren tatsächliche Cluster-Topologie ausreicht
   oder ob sie den Opt-in sofort brauchen.
4. **Timer-Overhead-Messung** — vor Rollout einen Benchmark mit
   `include-storage-listener-metrics=true` gegen ein realistisches Lastprofil fahren, um
   sicherzustellen, dass die Timer-Erfassung in `MorphiumMetricsStorageListener` nicht selbst
   zum Bottleneck wird (insbesondere bei `@WriteBuffer`-Batch-Workloads mit hoher Frequenz).
5. **`Capability.METRICS`-Migration (Folgearbeit, nicht blockierend für Phase 1):**
   `io.quarkus.deployment.Capability.METRICS` ist in Quarkus 3.32.3 als `@Deprecated`
   markiert (Javadoc verweist auf `io.quarkus.deployment.metrics.MetricsCapabilityBuildItem`).
   Phase 1 verwendet bewusst weiterhin `Capabilities.isPresent(Capability.METRICS)`, konsistent
   mit dem bestehenden Jackson/JSON-B-Gate-Muster in `MorphiumProcessor` (Abschnitt 2.2) und
   weil die Konstante in 3.32.3 voll funktionsfähig ist. Die Migration zu
   `MetricsCapabilityBuildItem` (ein `SimpleBuildItem` mit `MetricsCapability.isSupported(
   MetricsFactory.MICROMETER)` statt eines einfachen `isPresent(...)`-Checks — ein
   strukturell anderes `@BuildStep`-Signaturmuster) ist als eigenständiges Ticket für eine
   spätere Phase vorzumerken, idealerweise zusammen mit einer Überprüfung, ob das
   Jackson/JSON-B-Gate ebenfalls migriert werden soll, um innerhalb von `MorphiumProcessor`
   ein einheitliches Capability-Detection-Idiom zu behalten.

---

## 10. Zusammenfassung / Empfehlung

- **Kein neues Maven-Modul** — Erweiterung von `quarkus-morphium`/`quarkus-morphium-deployment`
  nach dem bereits im Repo etablierten und bewährten "optional dependency + Capabilities-Gate"-Muster
  (siehe Jackson/JSON-B-Präzedenzfall).
- **Registrierung ausschließlich im bestehenden `buildMorphium()`-Post-Connect-Hook**, niemals
  über einen frühen CDI-Observer, der den Lazy-Connect vorzeitig auslösen würde.
- **Kombination aus Gauges (Pull aus `getDriverStats()`/`getStatistics()`) und Counter/Timer
  (Push aus `MorphiumStorageListener`/`MorphiumTransactionEvent`)**, da Morphium-Core keine
  Latenz-Timer bereitstellt.
- **Kardinalität bewusst begrenzen** (kein Pro-Host-Tag per Default) und **Idempotenz bei
  Hot-Reload explizit lösen** (Meter-Deregistrierung im Shutdown-Pfad) — beides sind die zwei
  konkreten Fallstricke, die dieses Modul von einer naiven Umsetzung unterscheiden.
- Tracing/OTel-Spans bewusst als separates Folgeprojekt ausgeklammert.
