open Akka.Actor
open Akka.FSharp
open System.Threading

let system = ActorSystem.Create "MySystem"
let mutable Boss = null
let mutable workerArray = null 
let mutable timeNum = 0.0
let mutable numNodes = 0
let mutable childDoneState:int list = []
type Data = 
    | InputString of str:string
    | InputData of numNodes:int * topology:string * alg:string
let updateElement index element list = 
    list |> List.mapi (fun i v -> if i = index then element else v)

let workerActor (mailbox: Actor<_>) = 
    let mutable rumor = ""
    let mutable array:string list = [] 
    let mutable myCount = 0
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable ratio = 0.0
    let mutable ratio_old = 999.0
    let mutable dratio = 999.0
    let mutable terminateCount = 0
    let mutable gossipMode:bool = true
    let rec loop () = actor {
        let! message = mailbox.Receive ()
        // Handle message here
        //printfn "receive %A" message
        match box message with
        | :? string as firstStr -> 
            //printfn "Hello %A" firstStr.[0]
            match firstStr.[0] with
            | 'g' ->
                gossipMode <- true
                rumor <- firstStr.[1..]
                let sendmsg = "d"+ array.[0]
                Boss <! InputString(sendmsg)
                //transmitMsg
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                //printfn "arr:%A" array  
                //printfn "len: %A" len

                let mutable rndInt = System.Int32.MaxValue
   
                while len<>0 && ( rndInt > len || rndInt = 0) do
                    rndInt <- abs(System.Random().Next()%len)
                let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system
                targetWorker <! msg1
                //send message to itself
                let targetWorker1 = select (mailbox.Self.Path.ToString()) system       
                targetWorker1 <! "z"
            | 'p' ->
                gossipMode <- false
                s <- float array.[0]
                ratio <- s/w
                dratio <- abs (ratio_old-ratio)
                //transmitMsg()
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                let mutable rndInt = System.Int32.MaxValue
   
                while rndInt > len || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%len)
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

                    //transmitMsg()
                    let mutable msg1 =null
                    if gossipMode = true then
                        msg1 <- "rt" + rumor
                    else
                        msg1 <- "rf," + s.ToString()+","+w.ToString() 
                    let len = array.Length
                    let mutable rndInt = System.Int32.MaxValue
                    while  len<>0 && ( rndInt > len || rndInt = 0) do
                        rndInt <- abs(System.Random().Next()%len)
                    let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                    targetWorker <! msg1

                if gossipMode = true  && myCount<9 then
                    //Thread.Sleep(50)
                    //send message to self (keep gossip)
                    let targetWorker = select (mailbox.Self.Path.ToString()) system       
                    targetWorker <! "z"
                    //Thread.Sleep(20000)

                elif myCount=10 then
                    myCount <- myCount+1
                    let sendmsg = "d"+ array.[0]
                    Boss<! InputString(sendmsg)
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
                        while  len<>0 && ( rndInt > len || rndInt = 0) do
                            rndInt <- abs(System.Random().Next()%len)
                        let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                        targetWorker <! msg1
                    else
                        printfn "Terminated actor is getting messages"
            | 'l'->
                let str = firstStr.[1..].Split ','
                let len = str.Length
                
                for i=len-1 downto 0 do
                    if str.[i] <> "" then
                        array <- str.[i] :: array
                //printfn "arr:%A" array    
            | 'f' ->
                let input = firstStr.[1..].Split ','
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
                
            | 'm' ->
                let arr = firstStr.[1..].Split ','
                let len = arr.Length
                for i=len-1 downto 0 do
                   array <- arr.[i] :: array
            | 'z' ->
                //transmitMsg()
                let mutable msg1 =null
                if gossipMode = true then
                    msg1 <- "rt" + rumor
                else
                    msg1 <- "rf," + s.ToString()+","+w.ToString()
                let len = array.Length
                let mutable rndInt = System.Int32.MaxValue
   

                while rndInt > len || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%len)
                let targetWorker = select ("akka://MySystem/user/worker"+array.[rndInt]) system       
                targetWorker <! msg1
                //Thread.Sleep(50)
                //send message to self (keep gossip)
                let targetWorker = select (mailbox.Self.Path.ToString()) system       
                targetWorker <! "z"
            
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
                
                if revMsg.Length<=1 then //gossip mode
                    
                    let percentCovered = (float childCount)*100.0/(float numNodes)
                    //printfn "percent %A" percentCovered
                    if percentCovered = 100.0 then
                        printfn "stop"
                        
                        stopWatch.Stop()
                        printfn "time:%f ms" stopWatch.Elapsed.TotalMilliseconds
                        
                        timeNum<-stopWatch.Elapsed.TotalMilliseconds
                        for actor in workerArray do
                            system.Stop(actor)
                        Thread.Sleep(500)
                        printfn "\nPlease press any key to exit." 
                        system.Stop(Boss)
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
            | _ ->failwith "unknown message"
        | InputData(numNodes,topology,alg) ->
            //create worker actors
            workerArray <- Array.create numNodes (spawn system "workers" workerActor )
            for i=1 to numNodes do
                workerArray.[i-1] <- spawn system ("worker"+i.ToString()) workerActor
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
                //numNodes <-actualN
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
                     //printfn "msg %A" message
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
            match alg.ToLower() with
            | "gossip" ->
                //printfn "match gossip"
                let message:string = "g"+"findgossip"
                let mutable rndInt = System.Int32.MaxValue
                while rndInt > numNodes || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%numNodes)
                //printfn "random:%A" rndInt
                let targetWorker = select ("akka://MySystem/user/worker"+rndInt.ToString()) system
                targetWorker <! message
                //printfn "sent message"
            | "push-sum" ->
                let message:string = "p"+"0.0,0.0"
                let mutable rndInt = System.Int32.MaxValue
                while rndInt > numNodes || rndInt = 0 do
                    rndInt <- abs(System.Random().Next()%numNodes)
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

    topology <- topology.ToLower()
    if proj2 = "project2" && numNodes>0 && (topology = "full" || topology ="2d" || topology ="line" || topology ="imp2d") && (alg ="gossip"||alg= "push-sum") then
        if topology = "2d" || topology = "imp2d" then
            let mNumber = int (floor (sqrt (float numNodes)))
            let actualN = int (float mNumber ** 2.0)
            numNodes <-actualN
        
        for i in 0 .. numNodes do
            childDoneState  <- [0] |> List.append childDoneState
        //let arg0 : obj[] = [|int(NodeNum);string("line");string("gossip")|]
        Boss <- spawn system "boss" BossActor
        Boss <! InputData(numNodes,topology,alg)

        System.Console.ReadLine() |> ignore
        printfn "Final time:%f ms" timeNum
    else
        printfn "Wrong input."
    //System.Console.ReadLine() |> ignore
    0 // return an integer exit code