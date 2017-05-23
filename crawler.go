package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"compress/gzip"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"

	//"github.com/gogo/protobuf/proto"

	"github.com/satori/go.uuid"
	"time"
)

// the crawler is trying to maintain a guess at somewhere around trippleBound total tripples,
// while completing all movies (i.e. the director and all actors are set)
// and completing all directors (i.e. all their movies are set)
//
// possible other edges:
// film -> featured_locations -> name   ... but no geo data in here
// film -> sequel						... no queries to use it yet

type director struct {
	Name string
}
type actor director
type genre director

// FIXME add de and it language tags

type movie struct {
	Director    []uint64
	Name        string
	NameDE      string
	NameIT      string
	Starring    []performance
	Genre       []uint64
	ReleaseDate time.Time
}

type performance struct {
	Fst uint64 // UID of the actor
	Snd string // name of the character
}

const (
	trippleBound = 1100000
	//trippleBound = 1100000
)

var (
	directorSeeds = []string{"Peter Jackson", "Jane Campion", "Ang Lee", "Marc Caro", "Jean-Pierre Jeunet", "Tom Hanks", "Steven Spielberg",
		"Cherie Nowlan", "Hermine Huntgeburth", "Tim Burton", "Tyler Perry"}

	directors = make(map[uint64]director)
	actors    = make(map[uint64]actor)
	movies    = make(map[uint64]movie)
	genres    = make(map[uint64]string)

	tripplesEstimate = 0

	toVisit chan uint64 // queue of directors to visit

	dgraph = flag.String("d", "127.0.0.1:8080", "Dgraph server address")

	outfile = "1million.rdf.gz"

	directorByNameTemplate = `query directorTemplate($a: string)
{
	director(func: allofterms(name, $a)) @cascade {
		name
		director.film 
	}
}`
	directorByNameMap = make(map[string]string)

	directorsMoviesTemplate = `{
	movies(id: $a) @cascade {
		name@en
		director.film { 
			name@en 
			name@de
			name@it
      		starring {
        		performance.actor {
          			name@en
          		}
				performance.character {
					name@en
				}
			}
			genre {
				name@en
			}
			~director.film
			initial_release_date 
		}
	}
}`
	directorMoviesByIDMap = make(map[string]string)

	actorByIDTemplate = `{
	actor(id: $a) @cascade {
		actor.film {
			performance.film {
				~director.film
			 }
		}
	}
}`
	actorIDMap = make(map[string]string)
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	toVisit = make(chan uint64)

	conn, err := grpc.Dial(*dgraph, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	dgraphClient := protos.NewDgraphClient(conn)

	for _, dir := range directorSeeds {
		directorByNameMap["$a"] = dir

		resp, err := runQueryWithVariables(dgraphClient, directorByNameTemplate, directorByNameMap)
		if err != nil {
			log.Printf("Finding director %s.  --- Error in getting response from server, %s.", dir, err)
		} else {
			// respt.N[0] is always "_root_" ?
			// -> then it's children are Nodes for the query answers, one for each matched _uid_
			for _, child := range resp.N[0].Children {
				enqueueDirector(getUID(child.Properties))
			}
		}
	}

	// the seed directors are set up, now lets start to crawl
	// make this multithreaded??? ... Would need locks around the maps?
	crawl(1, dgraphClient)

	f, err := os.Create(outfile)
	if err != nil {
		log.Fatalf("Couldn't open file %s", err)
	}
	defer f.Close()

	toFile := gzip.NewWriter(f)
	defer toFile.Close()

	totalTripples := 0

	// blank nodes aren't working properly with dgraphloader
	// for now, this writes them out as XIDs

	toFile.Write([]byte("\n\t\t# -------- directors --------\n\n"))
	for dirID, dir := range directors {
		toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@en .\n", dirID, dir.Name)))
		totalTripples++
	}
	log.Printf("Wrote %v directors to %v", len(directors), outfile)

	toFile.Write([]byte("\n\t\t# -------- actors --------\n\n"))
	for actorID, act := range actors {
		toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@en .\n", actorID, act.Name)))
		totalTripples++
	}
	log.Printf("Wrote %v actors to %v", len(actors), outfile)

	toFile.Write([]byte("\n\t\t# -------- genres --------\n\n"))
	for genreID, genre := range genres {
		toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@en .\n", genreID, genre)))
		totalTripples++
	}
	log.Printf("Wrote %v genres to %v", len(genres), outfile)

	toFile.Write([]byte("\n\t\t# -------- movies --------\n\n"))

	for movieUID, movieData := range movies {
		toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@en .\n", movieUID, movieData.Name)))
		totalTripples++
		if movieData.NameDE != "" {
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@de .\n", movieUID, movieData.NameDE)))
			totalTripples++
		}
		if movieData.NameIT != "" {
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@it .\n", movieUID, movieData.NameIT)))
			totalTripples++
		}

		for _, dirID := range movieData.Director {
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <director.film> <%v> .\n", dirID, movieUID)))
			totalTripples++
		}

		for _, genreID := range movieData.Genre {
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <genre> <%v> .\n", movieUID, genreID)))
			totalTripples++
		}

		toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <initial_release_date> \"%v\" .\n", movieUID, movieData.ReleaseDate.Format(time.RFC3339))))
		totalTripples++

		for _, p := range movieData.Starring {
			pBlankNode := uuid.NewV4()
			charBlankNode := uuid.NewV4()

			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <starring> <%v> .\n", movieUID, pBlankNode)))
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <performance.film> <%v> .\n", pBlankNode, movieUID)))

			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <performance.actor> <%v> .\n", pBlankNode, p.Fst)))
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <actor.film> <%v> .\n", p.Fst, pBlankNode)))

			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <performance.character> <%v> .\n", pBlankNode, charBlankNode)))
			toFile.Write([]byte(fmt.Sprintf("\t\t<%v> <name> \"%v\"@en .\n", charBlankNode, p.Snd)))
			totalTripples += 6
		}
		toFile.Write([]byte(fmt.Sprintln()))
	}
	log.Printf("Wrote %v movies to %v", len(movies), outfile)
	log.Printf("Wrote %v tripples in total to %v", totalTripples, outfile)
}

// ---------------------- Dgraph queries ----------------------

func getContext() context.Context {
	return metadata.NewContext(context.Background(), metadata.Pairs("debug", "true"))
}

func runQueryWithVariables(dgraphClient protos.DgraphClient, query string, varMap map[string]string) (*protos.Response, error) {
	req := client.Req{}
	req.SetQueryWithVariables(query, varMap)
	return dgraphClient.Run(getContext(), req.Request())
}

func getPropertyIdx(properties []*protos.Property, edge string) int {
	for i, prop := range properties {
		if prop.Prop == edge {
			return i
		}
	}
	return -1
}

func getProperty(properties []*protos.Property, edge string) (*protos.Value, bool) {
	if index := getPropertyIdx(properties, edge); index != -1 {
		return properties[index].Value, true
	}
	return nil, false
}

// how to interograte this oneof properly ???
func getPropertyAsString(properties []*protos.Property, edge string) string {
	if val, ok := getProperty(properties, edge); ok {
		return val.GetStrVal()
	}
	return ""
}

func getUID(properties []*protos.Property) uint64 {
	if ID, ok := getProperty(properties, "_uid_"); ok {
		return ID.GetUidVal()
	}
	return 0
}

// just print with depth tabs in front
func printNode(depth int, node *protos.Node) {

	fmt.Println(strings.Repeat(" ", depth), "Atrribute : ", node.Attribute)
	//fmt.Println(strings.Repeat(" ", depth), "Uid : ", node.Uid)

	// the values at this level
	for _, prop := range node.GetProperties() {
		fmt.Println(strings.Repeat(" ", depth), "Prop : ", prop.Prop, " Value : ", prop.Value, " Type : %T", prop.Value)
	}

	for _, child := range node.Children {
		fmt.Println(strings.Repeat(" ", depth), "+")
		printNode(depth+1, child)
	}

}

// ---------------------- crawl movies ----------------------

// assume only one crawler for now
func crawl(numCrawlers int, dgraphClient protos.DgraphClient) {
	done := false

	for !done {
		select {
		case dirID := <-toVisit:
			visitDirector(dirID, dgraphClient)
		default:
			done = true
		}
	}

}

func enqueueDirector(dirID uint64) {
	if _, ok := directors[dirID]; !ok {
		go func() { toVisit <- dirID }()
	}
}

func visitDirector(dir uint64, dgraphClient protos.DgraphClient) {

	// if _, ok := directors[dir]; !ok {  // should always be fine, only put them in the queue if they haven't been visited

	if tripplesEstimate < trippleBound {

		directorMoviesByIDMap["$a"] = fmt.Sprint(dir)

		resp, err := runQueryWithVariables(dgraphClient, directorsMoviesTemplate, directorMoviesByIDMap)
		if err != nil {

			log.Printf("Error processing director : %v.  --- Error in getting response from server, %s", dir, err)
			return
		}

		if len(resp.N) > 0 && len(resp.N[0].Children) == 1 { // we've asked by ID, so can only be 1 if there is anything

			directors[dir] = director{getPropertyAsString(resp.N[0].Children[0].Properties, "name@en")}

			log.Println("Visiting director : ", dir, " - ", directors[dir].Name)

			// for each director.film edge
			for _, dirsMovie := range resp.N[0].Children[0].Children {

				var actorID uint64
				var actorName string
				var character string

				mov := movie{
					Director: make([]uint64, 0),
					Starring: make([]performance, 0),
					Genre:    make([]uint64, 0),
				}

				movieUID := getUID(dirsMovie.Properties)

				mov.Name = getPropertyAsString(dirsMovie.Properties, "name@en")
				mov.NameDE = getPropertyAsString(dirsMovie.Properties, "name@de")
				mov.NameIT = getPropertyAsString(dirsMovie.Properties, "name@it")

				log.Println("Found movie : ", movieUID, " - ", mov.Name)

				if ird, ok := getProperty(dirsMovie.Properties, "initial_release_date"); ok {
					// Seems like GetDateVal() doesn't work - it always returned empty bytes.
					if date, err := time.Parse(time.RFC3339, string(ird.GetStrVal())); err == nil {
						mov.ReleaseDate = date
					} else {
						log.Println("Error reading date : ", err)
					}
				}

				tripplesEstimate += 4

				for _, movieProp := range dirsMovie.Children {

					switch movieProp.Attribute { // switch on edge that led to this node

					case "starring":

						for _, stChild := range movieProp.Children {
							switch stChild.Attribute {
							case "performance.actor":
								actorID = getUID(stChild.Properties)
								actorName = getPropertyAsString(stChild.Properties, "name@en")
								visitActor(actorID, actorName, dgraphClient)
							case "performance.character":
								character = getPropertyAsString(stChild.Properties, "name@en")
							}
						}
						mov.Starring = append(mov.Starring, performance{actorID, character})

						tripplesEstimate += 4

					case "genre":

						if genreVal, ok := getProperty(movieProp.Properties, "name@en"); ok {
							genres[getUID(movieProp.Properties)] = genreVal.GetStrVal()
							mov.Genre = append(mov.Genre, getUID(movieProp.Properties))
						}

						tripplesEstimate++

					case "~director.film":
						mov.Director = append(mov.Director, getUID(movieProp.Properties))
						tripplesEstimate++
					}

				}
				movies[movieUID] = mov
			}
		}
	}
}



func visitActor(actorID uint64, name string, dgraphClient protos.DgraphClient) {

	if _, ok := actors[actorID]; !ok {

		actorIDMap["$a"] = fmt.Sprint(actorID)

		if tripplesEstimate < trippleBound {
			resp, err := runQueryWithVariables(dgraphClient, actorByIDTemplate, actorIDMap)
			if err != nil {
				log.Printf("Error processing actor : %v.  --- Error in getting response from server, %s", actorID, err)
				return
			}

			log.Println("Visiting actor : ", actorID, " - ", name)
			actors[actorID] = actor{name}
			tripplesEstimate++

			if len(resp.N) > 0 && len(resp.N[0].Children) == 1 { // asked by UID, so only one

				// for each performance in each movie they have acted in - actor.film
				for _, actorsMovie := range resp.N[0].Children[0].Children {

					// should only be one edge to a film from a performance - performance.film
					for _, actorPerformance := range actorsMovie.Children {

						// might be multiple directors, so
						// for each director in the movie - ~director.film
						for _, dir := range actorPerformance.Children {
							enqueueDirector(getUID(dir.Properties))
						}
					}
				}
			}
		}
	}
}




// // there must be a better way to do this in go?? ... oh found it strings.Repeat 
// func nStr(n int, s string) (res string) {
// 	res = ""
// 	for i := 0; i < n; i++ {
// 		res = fmt.Sprintf("%s%s", res, s)
// 	}
// 	return
// }
