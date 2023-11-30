# NoSQL Proyecto Final: Rick and Morty
![Alt text](https://m.media-amazon.com/images/M/MV5BYmY1MjU3N2MtMTA2Ny00YzlmLWIwY2EtM2I2MGQyMzY3YTA3XkEyXkFqcGdeQXVyMTEzOTc2MzQ3._V1_.jpg)


### Objetivo

Que los alumnos muestren el entendimiento y capacidad de manejo de las diferentes bases de datos que vimos en el semestre, así como los conceptos relacionados con API's, ETL's, etc.

### Planteamiento

Buscar alguna API que llame la atención al equipo, con esta API, conectarla através de python con una base de datos MongoDB. Posteriormente, hacer un ETL que cargue la base de datos procesada  una base de datos estilo grafo; evidentemente las transformaciones y los subconjuntos de datos ocupados serán diferentes para cada base de datos ya que tienen fines diferentes cada una.

# Equipo
- Mauricio Diaz
- Monica Gonzalez
- Lucia Varela

# Implementación

## Extract

En la fase de extracción, utilizamos el [RickandMortyAPI](https://rickandmortyapi.com/) para obtener información detallada sobre los personajes, ubicaciones y episodios de la serie. A través de solicitudes HTTP, recuperamos datos JSON de la API, que luego son procesados y almacenados en una base de datos MongoDB. Este paso sienta las bases para la posterior transformación y carga de datos.

# Transform

## Transformación de Datos

La etapa de transformación es crucial para garantizar que los datos obtenidos de la API estén en el formato adecuado y sean consistentes. Utilizando la biblioteca Pandas en Python, creamos un proceso de transformación que extrae los datos de la base de datos MongoDB y los organiza en un archivo CSV. Durante este proceso, también realizamos operaciones de limpieza y estructuración para garantizar la coherencia y calidad de los datos. Aqui tambien estamos haciendo reglas de normalizacion a las bases de datos. Este archivo CSV se convierte en una fuente de datos listos para su carga en diversas plataformas.

# Load

## Carga de Datos

Una vez que los datos han sido transformados y están en un formato adecuado, procedemos a cargarlos en dos sistemas de gestión de bases de datos diferentes: Neo4j y Cassandra.

### Neo4j

Neo4j es utilizado para almacenar datos en forma de grafos. Los personajes, ubicaciones y episodios se modelan como nodos, y las relaciones entre ellos se representan como bordes.

### Cassandra

Cassandra, por otro lado, se elige por su capacidad de escalabilidad y tolerancia a fallos. Los datos transformados se cargan en Cassandra, aprovechando su modelo de almacenamiento distribuido. Esto garantiza un acceso rápido y eficiente a los datos, especialmente en entornos con grandes volúmenes de información.

En resumen, este proceso de implementación abarca desde la extracción de datos de la API hasta su transformación y finalmente su carga en diferentes sistemas de almacenamiento, permitiendo un análisis posterior y el uso eficiente de la información recolectada.

### Queries en mongo
Dime los personajes que son hombres y son humanos
```
db.characters.aggregate([
{$match: {species: 'Human', gender:'Male'}},
{$project:{_id:0,species:1, gender:1, name:1}}])
```
Dime cuantos personajes vivos hay en cada lugar de origen
```
db.characters.aggregate([
{$unwind: '$origin'},
{$project:{_id:0, 'origin.name':1, "status":1}},
{$match:{status:'Alive'}},
{$group:{_id:"$origin.name", conteo:{$sum:1}}}
])
```
Dime las locaciones en las que viven los humanos y los Aliens y dime cuantos personajes hay en total en dichas ubicaciones 
```
db.characters.aggregate([
{$unwind:"$location"},
{$project:{"location.name":1, "id":1, "species":1}},
{$group:{_id:"$species", "locationArray":{$push:"$location.name"}}},
{$project:{"name":"$_id",_id:1,"locationArray":1}},
{$addFields:{"totalHabitantes":{$sum:1}}}])
```

### Queries en neo4j
En la API de rick and morty para neo4j es solo necesario hacer 2 relaciones ya que tenemos las tablas de episodes, location y characters
Relacion de episodios con personajes
```
        MATCH (c:character),(e:episode)
        WHERE c.episode_id = e.episode_id
        CREATE (c)-[r:APPEARED_IN]->(e)
```
Relacion de personajes con ubicaciones
```
        MATCH (c:character),(l:location)
        WHERE c.character_id = l.character_id
        CREATE (c)-[r:LOCATED_IN]->(l)
```

## Queries de analisis
query que nos ayuda a ver cuantos personajes son humanos
```
                            MATCH (character:character)
                            WHERE character.species = "Human"
                            RETURN character
```
query que nos ayuda a ver que personajes estuvieron en los episodios de meesseeks and destroy y mortys mind blowers
```
MATCH (character:character)-[:APPEARED_IN]->(episode:episode)
                            WHERE episode.name IN ["Meeseeks and Destroy", "Morty's Mind Blowers"]
                            RETURN character
```

Ubicaciones average por episodio
```
MATCH (e:episode)-[:APPEARED_IN]->(c:character)-[:LOCATED_IN]->(l:location)
WITH e, COUNT(DISTINCT l) AS locationCount
RETURN AVG(locationCount) AS avgLocationsPerEpisode

```

### Queries en cassandra
muestra todos los nombres de todos los episodios
```
SELECT name FROM episodes;
```
muestra los datos del personaje 671

```
SELECT * FROM characters WHERE character_id=671;
```
Muestra los datos de todas las ubicaciones
```
SELECT * FROM locations;
```

### Instrucciones para el uso
1. Abre la terminal, hazle un fork al repo
2. Entra al directorio del repo
3. Corre los siguientes comandos(tarda un ratito)
```
docker-compose build
docker-compose up
```
Esto correra, todo el proceso de etl, e imprimira el resultado de las queries.
Otra manera es con el notebook, que tenemos donde corres los codigos, mientras tienes loss conteneddores de mongo, cassandra y neo4j y te regresan los resultados.


