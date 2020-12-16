open Akka.Actor
open Akka.FSharp
open System.Threading

let system = ActorSystem.Create "MySystem"
let mutable Boss = null
let mutable workerArray = null 
let mutable timeNum = 0.0
let mutable numNodes = 0
let mutable nKill = 0
let mutable numKill = 0
let mutable convergence = 0.0
let mutable childDoneState:int list = []
let mutable deadLines:string list = []
type Data = 
    | InputString of str:string
    | InputData of numNodes:int * topology:string * alg:string
let updateElement index element list = 
    list |> List.mapi (fun i v -> if i = index then element else v)

let workerActor (mailbox: Actor<_>) = 
    let mutable rumor = ""
    let mutable array:string list = [] 
    let mutable myDeadLines:string list = []
    let mutable myCount = 0
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable ratio = 0.0
    let mutable ratio_old = 999.0
    let mutable dratio = 999.0
    let mutable terminateCount = 0
    let mutable gossipMode:bool = true
    let rec loop () = actor {
        //let mutable gossipMode:bool = true
        let! message = mailbox.Receive ()
        //let mutable parentNode = null//mailbox.Sender()
        // Handle message here
        //printfn "receive !! %A" message
        match box message with
        | :? string as firstStr -> 
            //printfn "Hello %A" firstStr.[0]
            match firstStr.[0] with    
            | 'g' ->
                //printfn "g"
                //printfn "match g"
                gossipMode <- true
                rumor <- firstStr.[1..]
                let sendmsg = "d"+ array.[0]
                Boss <! InputString(sendmsg)
                //parentNode <! InputString(sendmsg)
                //transmitMsg()
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                //printfn "arr:%A" array  
                //printfn "len: %A" len

                let mutable rndInt = System.Int32.MaxValue
   
                //find a random actor to send message
                //if the actor is killed, find another one 
                while len<>0 && ( rndInt > len || rndInt = 0) do
                    rndInt <- abs(System.Random().Next()%len)
                    if myDeadLines.Length > 0 then
                        for elem in myDeadLines do
                            if int(array.[rndInt]) = int elem then
                                //printfn "RANDOM %A" rndInt
                                rndInt <- 0


                let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                //printfn "msg1:%A" msg1
                targetWorker <! msg1
                let targetWorker1 = select (mailbox.Self.Path.ToString()) system       
                targetWorker1 <! "z"
            | 'p' ->
                gossipMode <- false
                s <- float array.[0]
                ratio <- s/w
                dratio <-abs (ratio_old-ratio)
                //transmitMsg()
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                let mutable rndInt = System.Int32.MaxValue
   

                while len<>0 && ( rndInt > len || rndInt = 0) do
                    rndInt <- abs(System.Random().Next()%len)
                    if myDeadLines.Length > 0 then
                        for elem in myDeadLines do
                            if int(array.[rndInt]) = int elem then
                                //printfn "RANDOM %A" rndInt
                                rndInt <- 0
                let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                targetWorker <! msg1
            | 'r' ->
                match firstStr.[1] with
                | 't' -> gossipMode <- true
                | 'f' -> gossipMode <- false
                | _ -> failwith "unknown message" 
                //printfn "%A count: %A" mailbox.Self.Path myCount
                //printfn "count: %A" myCount
                if gossipMode = true && myCount<10 then
                    myCount <- myCount+1
                    rumor <- firstStr.[2..]
                    let sendmsg = "d"+ array.[0]
                    Boss<! InputString(sendmsg)
                    //parentNode <! InputString(sendmsg)

                    //transmitMsg()
                    let mutable msg1 =null
                    if gossipMode = true then
                        msg1 <- "rt" + rumor
                    else
                        msg1 <- "rf," + s.ToString()+","+w.ToString() 
                    let len = array.Length
                    let mutable rndInt = System.Int32.MaxValue
                    while len<>0 && ( rndInt > len || rndInt = 0) do
                        rndInt <- abs(System.Random().Next()%len)
                        if myDeadLines.Length > 0 then
                            for elem in myDeadLines do
                                if int(array.[rndInt]) = int elem then
                                    //printfn "RANDOM %A" rndInt
                                    rndInt <- 0
                    let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                    targetWorker <! msg1

                if gossipMode = true && myCount < 9 then
                    //Thread.Sleep(50)
                    //send message to self (keep gossip)
                    let targetWorker = select (mailbox.Self.Path.ToString()) system       
                    targetWorker <! "z"
                elif myCount=10 then
                    myCount <- myCount+1
                    let sendmsg = "d"+ array.[0]
                    Boss<! InputString(sendmsg)
                    //parentNode <! InputString(sendmsg)

                    //printf "%A" mailbox.Self.Path
                    //printfn "is done."
                    system.Stop(mailbox.Self)
                elif gossipMode = false then
                    if s = 0.0 then
                        s <- float array.[0]
                    let inParams = firstStr.[2..].Split ','
                    s <- (s + float inParams.[1])/2.0
                    w <- (w + float inParams.[2])/2.0
                    ratio_old <- ratio
                    ratio <- s/w
                    //printfn "ratio %A" ratio
                    dratio<-abs (ratio-ratio_old)
                    if dratio < 0.0000000001 then
                        terminateCount <- terminateCount+1
                    else
                        terminateCount <- 0
                    if terminateCount = 3 then  //done
                        let sendmsg = "d"+ array.[0]+","+ratio.ToString()

                        Boss <! InputString(sendmsg)
                    elif terminateCount < 3 then
                        let mutable msg1 =null
                        if gossipMode = true then
                            msg1 <- "rt" + rumor
                        else
                            msg1 <- "rf," + s.ToString()+","+w.ToString()
                        let len = array.Length
                        let mutable rndInt = System.Int32.MaxValue
                        while len<>0 && ( rndInt > len || rndInt = 0) do
                            rndInt <- abs(System.Random().Next()%len)
                            if myDeadLines.Length > 0 then
                                for elem in myDeadLines do
                                    if int(array.[rndInt]) = int elem then
                                        //printfn "RANDOM %A" rndInt
                                        rndInt <- 0
                        let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                        targetWorker <! msg1
                    else
                        printfn "Terminated actor is getting messages"
            | 'l'->
                //printfn "is line"
                let str = firstStr.[1..].Split ','
                let len = str.Length
                
                for i=len-1 downto 0 do
                    if str.[i] <> "" then
                        array <- str.[i] :: array
                //printfn "arr:%A" array
            | 'f' ->
                let input = firstStr.[1..].Split ','
                //let numNodes = int input.[0]
                let myNumber = input.[1]
                for i =numNodes downto 1 do
                    array <- i.ToString() :: array
                let myNumint = int myNumber
                array<-updateElement 0 myNumber array
                array<-updateElement (myNumint-1) "1" array
                    
            | '2' ->
                let arr = firstStr.[1..].Split ','
                let len = arr.Length
                for i=len-1 downto 0 do
                   array <- arr.[i] :: array
                //for ss in arr do
                 //   array <- ss :: array
            | 'm' ->
                let arr = firstStr.[1..].Split ','
                let len = arr.Length
                for i=len-1 downto 0 do
                   array <- arr.[i] :: array
                //for ss in arr do
                 //   array <- ss :: array
            | 'z' ->
                //transmitMsg()
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                let mutable rndInt = System.Int32.MaxValue
   

                while len<>0 && ( rndInt > len || rndInt = 0) do
                    rndInt <- abs(System.Random().Next()%len)
                    if myDeadLines.Length > 0 then
                        for elem in myDeadLines do
                            if int(array.[rndInt]) = int elem then
                                //printfn "RANDOM %A" rndInt
                                rndInt <- 0
                let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                targetWorker <! msg1
                //Thread.Sleep(50)
                //send message to self (keep gossip)
                let targetWorker = select (mailbox.Self.Path.ToString()) system       
                targetWorker <! "z"
            | 'a' -> // node is deleted update coming in from boss
                let arr = firstStr.[1..].Split ','
                
                let len = arr.Length
                for i=len-1 downto 0 do
                   myDeadLines <- arr.[i] :: myDeadLines
            | 'j' -> //parent is killing you for testing node failure analysis
                let sendmsg = "x"+ array.[0]
                Boss <! InputString(sendmsg)
                system.Stop(mailbox.Self)
            | _ ->failwith "unknown message" 
        | _ ->  
            //printfn "print:%A" message
            failwith "unknown message"
    
        return! loop ()
    }
    loop ()

let BossActor (mailbox: Actor<_>) = 
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
    let rec loop () = actor {
        let! msg = mailbox.Receive ()
        match msg with
        | InputString(str)->
            match str.[0] with
            | 'd' ->
                let revMsg = str.[1..].Split ','
                let revInt = (int revMsg.[0])-1
                childDoneState <- updateElement revInt 1 childDoneState
                //printfn "child: %A" childDoneState
                let mutable childCount = 0
                for x in childDoneState do
                    if x = 1 then
                        childCount<- childCount+1
                //printfn "time"
                if revMsg.Length<=1 then //gossip mode
                    
                    let percentCovered = (float childCount)*100.0/(float (numNodes-numKill))
                    
                    convergence <- percentCovered
                    //printfn "percent %A" percentCovered
                    if percentCovered = 100.0 then
                        printfn "stop"
                        stopWatch.Stop()
                        printfn "time:%f ms" stopWatch.Elapsed.TotalMilliseconds
                        timeNum<-stopWatch.Elapsed.TotalMilliseconds
                        for actor in workerArray do
                            system.Stop(actor)
                        Thread.Sleep(500)
                        if convergence <> 0.0 then
                            printfn "convergence: %A%%" convergence
                        printfn "\nPlease press any key to exit."
                        system.Stop(Boss)
                    else
                        //printfn "Forced to stop! time:%f ms" stopWatch.Elapsed.TotalMilliseconds
                        timeNum<-stopWatch.Elapsed.TotalMilliseconds
                            
                else //push-sum mode
                    printfn "stop"
                    stopWatch.Stop()
                    printfn "time:%f ms" stopWatch.Elapsed.TotalMilliseconds
                    timeNum<-stopWatch.Elapsed.TotalMilliseconds
                    for actor in workerArray do
                        system.Stop(actor)
                    Thread.Sleep(500)
                    printfn "\nPlease press any key to exit."
                    system.Stop(Boss)
            | 'e' ->
                printfn "stop"
            
            | 'x' -> //some child node dies
                //----Bonus Code------
                for actor in workerArray do
                    //printfn "str: %A" str.[1..]
                    actor <! "a"+str.[1..]

            | _ ->failwith "unknown message"
        | InputData(numNodes,topology,alg) ->
            workerArray <- Array.create numNodes (spawn system "workers" workerActor )
            for i=1 to numNodes do
                workerArray.[i-1] <- spawn system ("worker"+i.ToString()) workerActor
                //printfn "numnode:%A" i
                
            match topology.ToLower() with
            | "full" ->
                for i=1 to numNodes do
                     let mutable message:string = "f"+i.ToString()
                     let targetWorker = select ("akka://MySystem/user/worker"+i.ToString()) system
                     message <-  message + "," + i.ToString()
                     targetWorker <! message
            | "2d" ->
                let mNumber = int (floor (sqrt (float numNodes)))
                let actualN = int (float mNumber ** 2.0)
                for i=1 to actualN do
                     let mutable message:string = "2"+i.ToString()
                     let targetWorker = select ("akka://MySystem/user/worker"+i.ToString()) system
                     if i-mNumber>0 then
                        message<-message + "," + (i - mNumber).ToString()
                     if i+mNumber<=actualN then
                        message<-message + "," + (i + mNumber).ToString()
                     if i%mNumber=0 then
                        message<-message + "," + (i - 1).ToString()
                     elif i%mNumber=1 then
                         message<-message + "," + (i + 1).ToString()
                     else
                        message<-message + "," + (i - 1).ToString() + "," + (i + 1).ToString()
                     
                     targetWorker <! message
            | "line" ->  
                for i=1 to numNodes do
                    //printfn "%A" i
                    let mutable message:string = "l"+i.ToString()+","

                    let targetWorker = select ("akka://MySystem/user/worker"+i.ToString()) system

                    if i-1>0 then
                        message <- message+(i-1).ToString()+","
                    
                    if i<numNodes then
                        message <- message+(i+1).ToString()
                        //printfn "mesg: %A" message
                    targetWorker <! message
            | "imp2d" ->
                let mNumber = int (floor (sqrt (float numNodes)))
                let actualN = int (float mNumber ** 2.0)
                let mutable rndInt = System.Int32.MaxValue
   
                while rndInt > actualN || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%actualN)
                for i=1 to actualN do
                     let mutable message:string = "m"+i.ToString()
                     let targetWorker = select ("akka://MySystem/user/worker"+i.ToString()) system
                     if i-mNumber>0 then
                        message<-message + "," + (i - mNumber).ToString()
                     if i+mNumber<=actualN then
                        message<-message + "," + (i + mNumber).ToString()
                     if i%mNumber=0 then
                        message<-message + "," + (i - 1).ToString()
                     elif i%mNumber=1 then
                         message<-message + "," + (i + 1).ToString()
                     else
                        message<-message + "," + (i - 1).ToString() + "," + (i + 1).ToString()
                     
                     message <- message + "," + rndInt.ToString()

                     targetWorker <! message
            | _ ->failwith "unknown message"

            //wake and kill some nodes //----Bonus Code------
            printfn "kill %A nodes" nKill
            while nKill > 0 do
                nKill <- nKill - 1
                let mutable rndInt = System.Int32.MaxValue 
                while rndInt > numNodes || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%numNodes)

                deadLines <- rndInt.ToString() :: deadLines
                let targetWorker = select ("akka://MySystem/user/worker"+rndInt.ToString()) system
                //printfn "targetworker: %A" targetWorker
                targetWorker <! "j"
            Thread.Sleep(500)

            match alg.ToLower() with
            | "gossip" ->
                //printfn "match gossip"
                let message:string = "g"+"findgossip"
                let mutable rndInt = System.Int32.MaxValue
                while rndInt > numNodes || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%numNodes)
                    if deadLines.Length > 0 then
                        for elem in deadLines do
                            if rndInt = int elem then
                                rndInt <- 0
                //printfn "random:%A" rndInt
                let targetWorker = select (mailbox.Self.Path.ToString()) system       
                targetWorker <! "z"
                let targetWorker = select ("akka://MySystem/user/worker"+rndInt.ToString()) system
                targetWorker <! message
                //printfn "sent message"
            | "push-sum" ->
                let message:string = "p"+"0.0,0.0"
                let mutable rndInt = System.Int32.MaxValue
                while rndInt > numNodes || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%numNodes)
                    if deadLines.Length > 0 then
                        for elem in deadLines do
                            if rndInt = int elem then
                                rndInt <- 0
                let targetWorker = select ("akka://MySystem/user/worker"+rndInt.ToString()) system
                targetWorker <! message
            | _ ->failwith "unknown message"

            
        return! loop ()
    }
    loop ()


[<EntryPoint>]  // Works also from F#-Interactive.
let main argv = 
    printfn "Please input values as format: \"project2 numNodes topology algorithm\""
    let line = System.Console.ReadLine()
    let arg = line.Split ' '
    let proj2 = string arg.[0]
    numNodes<-int arg.[1]
    let mutable topology = string arg.[2]
    let alg = string arg.[3]

    printfn "Please input a parameter between 0-100 (parameter as percent of failure)"
    let line1 = System.Console.ReadLine()
    let param = line1 |> int
    nKill <- int (numNodes * param/100)
    numKill <- int (numNodes * param/100)

    topology <- topology.ToLower()
    if proj2 = "project2" && numNodes>0 && (topology = "full" || topology ="2d" || topology ="line" || topology ="imp2d") && (alg ="gossip"||alg= "push-sum") then
        if topology = "2d" || topology = "imp2d" then
            let mNumber = int (floor (sqrt (float numNodes)))
            let actualN = int (float mNumber ** 2.0)
            numNodes <-actualN
        for i in 0 .. numNodes do
            childDoneState  <- [0] |> List.append childDoneState

        Boss <- spawn system "boss" BossActor
        //send message to self
        //Boss <! InputString("z")
        //Thread.Sleep(500)

        Boss <! InputData(numNodes,topology,alg)
        //System.Console.ReadLine() |> ignore
        Thread.Sleep(10000)

        if timeNum = 0.0 then
            printfn "push-sum failed."
        else
            printfn "Final time:%f ms" timeNum
        if convergence <> 0.0 then
            printfn "convergence: %A%%" convergence
    else
        printfn "Wrong input."
    //System.Console.ReadLine() |> ignore
    0 // return an integer exit code