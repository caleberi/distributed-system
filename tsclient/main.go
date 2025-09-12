package main

import (
	"time"

	"github.com/caleberi/distributed-system/client"
	"github.com/caleberi/distributed-system/common"
)

var data string = `### Captain America: The Winter Soldier - A Modern Marvel Masterpiece
**"Captain America: The Winter Soldier"** is a pivotal film in the Marvel Cinematic Universe (MCU), 
blending high-octane action, espionage, and character-driven storytelling to deliver a superhero movie that transcends the genre.

#### Plot and Themes
The film, directed by Anthony and Joe Russo, follows Steve Rogers, aka Captain America (played by Chris Evans), 
as he adjusts to life in the 21st century after being frozen in ice for decades. The narrative thrust begins when a SHIELD vessel is hijacked, 
prompting Rogers and Natasha Romanoff, aka Black Widow (Scarlett Johansson), to lead a rescue mission. This sets off a chain of events revealing a dee-seated conspiracy within SHIELD itself, orchestrated by the nefarious organization Hydra.
Central to the film's plot is the introduction of the Winter Soldier, a mysterious and formidable assassin with a metal arm and a shrouded past. As Rogers digs deeper, he discovers that the Winter Soldier is actually his old friend Bucky Barnes (Sebastian Stan), who has been brainwashed and manipulated by Hydra.

#### A New Direction for the MCU

"The Winter Soldier" is notable for its departure from the more fantastical elements of earlier Marvel films, opting instead for a tone reminiscent of 1970s political thrillers. 
The film explores themes of trust, freedom, and surveillance, reflecting contemporary concerns about government overreach and the balance between security and privacy.
This thematic depth is complemented by tightly choreographed action sequences. The film's fight scenes are visceral and grounded, particularly the close-quarters combat between Captain America and the Winter Soldier. The freeway fight and the elevator brawl are standout moments that have been praised for their intensity and choreography.

#### Character Development
Steve Rogers' character arc in this film is significant. No longer the wide-eyed patriot of "The First Avenger," Rogers is now a man out of time, 
grappling with the moral complexities of the modern world. His steadfast morality is tested as he navigates a landscape where enemies can be indistinguishable from allies.
Black Widow also receives substantial development, revealing layers of vulnerability and complexity beneath her spy persona. Her evolving partnership with Rogers adds emotional weight to the narrative,
as both characters confront their pasts and uncertain futures.

#### Impact on the MCU
"Captain America: The Winter Soldier" had a profound impact on the MCU. It effectively dismantled SHIELD, a cornerstone of the MCU's narrative infrastructure up to that point,
forcing subsequent films and TV shows to navigate a world without the organization's stabilizing presence.
The film also set the stage for "Avengers: Age of Ultron" and "Captain America: Civil War," influencing character motivations and the broader geopolitical landscape of the MCU. 
The introduction of the Winter Soldier as a tragic antagonist added emotional depth to future storylines, particularly those involving Steve Rogers.

#### Conclusion

"Captain America: The Winter Soldier" is more than just a superhero film; it is a sophisticated thriller that challenges its characters and audience to reconsider their views on heroism, 
loyalty, and sacrifice. Its successful blend of action, intrigue, and character development makes it one of the standout entries in the Marvel Cinematic Universe, and a benchmark for what 
superhero films can achieve when they aspire to be more than just spectacle.`

func main() {
	addr := "127.0.0.1:9090"
	client := client.NewClient(common.ServerAddr(addr), 30*time.Millisecond)
	defer client.Close()
	// handle, err := client.GetChunkHandle("/image/ice-79", common.ChunkIndex(0))
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Info().Msg(fmt.Sprintf("Got a new handle : %v", handle))
	// data := make([]byte, 1000)
	// _, err = client.Read("/image/ice-79", common.Offset(10), data)
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Print(string(data))

	// addr := "127.0.0.1:9090"
	// client := client.NewClient(common.ServerAddr(addr), 30*time.Millisecond)
	// defer client.Close()
	// handle, err := client.GetChunkHandle("/image/ice-79", common.ChunkIndex(0))
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Info().Msg(fmt.Sprintf("Got a new handle : %v", handle))
	// data := []byte(data)

	// err = client.Write("/image/ice-79", common.Offset(0), data)
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// addr := "127.0.0.1:9090"
	// client := client.NewClient(common.ServerAddr(addr), 30*time.Millisecond)
	// defer client.Close()
	// handle, err := client.GetChunkHandle("/image/ice-79", common.ChunkIndex(0))
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// log.Info().Msg(fmt.Sprintf("Got a new handle : %v", handle))
	// // data := []byte(data)

	// _, err = client.Append("/image/ice-79", []byte("The sector is the minimum storage unit of a hard drive.[1] Most disk partitioning schemes are designed to have files occupy an integral number of sectors regardless of the file's actual size. Files that do not fill a whole sector will have the remainder of their last sector filled with zeroes. In practice, operating systems typically operate on blocks of data, which may span multiple sectors.[2]"))
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// err = client.MkDir("/test-files/js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// err = client.CreateFile("/test-files/js/index.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// pathInfos, err = client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

	// err = client.DeleteFile("/test-files/js/index.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// pathInfos, err := client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

	// err = client.CreateFile("/test-files/js/app.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// err = client.RenameFile("/test-files/js/app.js", "/test-files/js/ok.js")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }

	// pathInfos, err := client.List("/")
	// if err != nil {
	// 	log.Err(err).Stack().Msg(err.Error())
	// 	return
	// }
	// utils.ForEach(pathInfos, func(v common.PathInfo) {
	// 	fmt.Println(">> " + v.Path)
	// })

}
