module Main

open Akka
open System
open Topologies
open Schedulers
open Akka.Actor
open Akka.Actor.Scheduler
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
open Akka.Configuration

// Set algo parameters here
let pushsumConvergenceCriteria = 15
let pushsumRoundsTimeMs = 50.0

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let gossipSystem = ActorSystem.Create("GossipSystem", configuration)
let mutable mainActorRef = null

type MainCommands =
    | FindNeighbours of (list<IActorRef> * string)
    | BuildTopology of (string)
    | GossipActorStoppedTransmitting of (string)
    | StartAlgorithm of (string)
    | GossipMessage of (int*string)
    | Round
    | IReceivedRumour of (int)
    | IFoundNeighbours of (int)
    | PushsumMessage of (int*float*float)
    | InitiatePushsum
    | PushsumActorConverged of (int*float*float)

//*** Globals ***//
// Default
let mutable numNodes = 27000
let mutable topology = "3D"
let mutable algorithm = "pushsum"
let mutable actorsPool = []
let mutable isImproper = false
let timer = Stopwatch()

// For 3D grid, rounding off the total nodes to next nearest cube
let roundOff3DNodes (numNodes:int) =
    let sides = numNodes |> float |> Math.Cbrt |> ceil |> int
    pown sides 3

// For 2D grid, rounding off the total nodes to next nearest square
let roundOff2DNodes (numNodes:int) =
    let sides = numNodes |> float |> sqrt |> ceil |> int
    pown sides 2


// Each actor (node) will call this function to maintain its own list of neighbours
let findMyNeighbours (pool:list<IActorRef>, topology:string, myActorIndex:int) = 
    let mutable myNeighbours = []
    match topology with 
    | "line" ->
        myNeighbours <- Topologies.findLineNeighboursFor(pool, myActorIndex, numNodes)
    | "2D" | "2d" | "imp2D" | "imp2d" ->
        let side = numNodes |> float |> sqrt |> int
        myNeighbours <- Topologies.find2DNeighboursFor(pool, myActorIndex, side, numNodes, isImproper)
    | "3D" | "3d" | "imp3D" | "imp3d" ->
        let side = numNodes |> float |> Math.Cbrt |> int
        myNeighbours <- Topologies.find3DNeighboursFor(pool, myActorIndex, side, (side * side), numNodes, isImproper)
    | "full" ->
        myNeighbours <- Topologies.findFullNeighboursFor(pool, myActorIndex, numNodes)
    | _ -> ()
    myNeighbours


let GossipActor (id:int) (mailbox:Actor<_>) =
    let mutable myNeighboursArray = []
    let mutable myRumourCount = 0
    let myActorIndex = id
    let mutable isActive = 1
 
    let rec loop () = actor {
        if isActive = 1 then
            let! (message) = mailbox.Receive()

            match message with 
            | FindNeighbours(pool, topology) ->
                myNeighboursArray <- findMyNeighbours(pool, topology, myActorIndex)
                mainActorRef <! IFoundNeighbours(myActorIndex)

            | GossipMessage(fromNode, message) ->
                if myRumourCount = 0 then
                    mainActorRef <! IReceivedRumour(myActorIndex)
                    mailbox.Self <! Round

                myRumourCount <- myRumourCount + 1

                if myRumourCount > 10 then
                    isActive <- 0
                    mainActorRef <! GossipActorStoppedTransmitting(mailbox.Self.Path.Name)

            | Round(_) ->
                // Find a random neighbour form neighbour list
                let randomNeighbour = Random().Next(myNeighboursArray.Length)
                let randomActor = myNeighboursArray.[randomNeighbour]
                // Send GossipMessage to it
                randomActor <! GossipMessage(myActorIndex, "rumour")
                gossipSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self, Round, mailbox.Self)
                
            | _ -> ()

            return! loop()
        }
    loop()


let PushsumActor (myActorIndex:int) (mailbox:Actor<_>) =
    let mutable myNeighboursArray = []
    let mutable s = myActorIndex + 1 |> float
    let mutable w = 1 |> float
    let mutable prevRatio = s/w
    let mutable currRatio = s/w
    let mutable towardsEnd = 0
    let mutable isActive = 1
    let mutable incomingsList = []
    let mutable incomingwList = []
    let mutable s_aggregateTminus1 = 0.0
    let mutable w_aggregateTminus1 = 0.0
    let mutable count = 0
 
    let rec loop () = actor {
        if isActive = 1 then    
            let! (message) = mailbox.Receive()

            match message with 
            | FindNeighbours(pool, topology) ->
                myNeighboursArray <- findMyNeighbours(pool, topology, myActorIndex)
                mainActorRef <! IFoundNeighbours(myActorIndex)

            | Round(_) ->
                
                // Step 2: s <- Σ(incomingsList) and w <- Σ(incomingwList)
                s <- s_aggregateTminus1
                w <- w_aggregateTminus1

                // Step 3: Choose a random neighbour
                let randomNeighbour = Random().Next(0, myNeighboursArray.Length)
                let randomActor = myNeighboursArray.[randomNeighbour]

                // Step 4: Send the pair ( ½s , ½w ) to randomNeighbour and self
                randomActor <! PushsumMessage(myActorIndex , (s/2.0) , (w/2.0))
                mailbox.Self <! PushsumMessage(myActorIndex , (s/2.0) , (w/2.0))

                // Check for convergence
                currRatio <- s / w
                if (abs(currRatio - prevRatio)) < (pown 10.0 -10) then 
                    towardsEnd <- towardsEnd + 1
                else 
                    towardsEnd <- 0
                
                if towardsEnd = pushsumConvergenceCriteria then 
                    mainActorRef <! PushsumActorConverged(myActorIndex, s,w)
                    isActive <- 0

                prevRatio <- currRatio

                // Reset the aggregate back to 0 after each round
                s_aggregateTminus1 <- 0.0
                w_aggregateTminus1 <- 0.0
                count <- 0
                       
            | PushsumMessage (fromIndex, incomings, incomingw) ->
                count <- count + 1
                s_aggregateTminus1 <- s_aggregateTminus1 + incomings
                w_aggregateTminus1 <- w_aggregateTminus1 + incomingw

            | InitiatePushsum ->
                mailbox.Self <! PushsumMessage(myActorIndex , s , w)
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(pushsumRoundsTimeMs), mailbox.Self, Round)
 
            | _ -> ()

            return! loop()
        }
    loop()

let MainActor (mailbox:Actor<_>) =    
    let mutable actorsDone = 0
    let mutable actorsThatKnow = 0
    let mutable topologyBuilt = 0

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | BuildTopology(_) ->
                printfn "BuildTopology"
                if algorithm = "gossip" then    
                    actorsPool <-
                        [0 .. numNodes-1]
                        |> List.map(fun id -> spawn gossipSystem (sprintf "GossipActor_%d" id) (GossipActor id))
                else
                    actorsPool <- 
                        [0 .. numNodes-1]
                        |> List.map(fun id -> spawn gossipSystem (sprintf "PushsumActor_%d" id) (PushsumActor id))

                actorsPool |> List.iter (fun item -> 
                    item <! FindNeighbours(actorsPool, topology))

            | IFoundNeighbours(index) ->
                topologyBuilt <- topologyBuilt + 1
                if topologyBuilt = numNodes then 
                    printfn "Topology completely built! \n"
                    timer.Start()
                    mainActorRef <! StartAlgorithm(algorithm)

            | StartAlgorithm(algorithm) ->
                //start algo
                match algorithm with 
                | "gossip" ->
                    // Randomly select a neighbour
                    let randomNeighbour = Random().Next(actorsPool.Length)
                    let randomActor = actorsPool.[randomNeighbour]
                    // Send GossipMessage to it
                    printfn "Sending firstrumour to %d\n" randomNeighbour
                    randomActor <! GossipMessage(0, "theRumour")
                | "pushsum" ->
                    // Initialize all
                    actorsPool |> List.iter (fun item -> 
                        item <! InitiatePushsum)
                | _ -> ()

            | GossipActorStoppedTransmitting(actorName) ->
                actorsDone <- actorsDone + 1
                (*printfn "%s Terminated | Total terminated = %d" actorName actorsDone
                if actorsDone = numNodes then 
                    printfn "\n SAB TERMINATED\n"
                    timer.Stop()
                    printfn "Total time = %dms" timer.ElapsedMilliseconds
                    Environment.Exit(0)*)

            | IReceivedRumour(actorIndex) ->
                actorsThatKnow <- actorsThatKnow + 1
                printfn "%d knows! Total = %d\n" actorIndex actorsThatKnow
                if actorsThatKnow = numNodes then 
                    timer.Stop()
                    printfn "\nTotal time = %d ms | Total Terminated = %d\n" timer.ElapsedMilliseconds actorsDone
                    printfn "\n\n Everyone knows the rumour!!\n"
                    Environment.Exit(0)

            | PushsumActorConverged (index, s, w) ->
                actorsDone <- actorsDone + 1
                printfn "id = %d | s = %f | w = %f | s/w = %f | Total terminated = %d"  index s w (s/w) actorsDone
                if actorsDone = numNodes then 
                    printfn "\n\n All nodes have converged!!\n\n"
                    timer.Stop()
                    printfn "Total time = %dms" timer.ElapsedMilliseconds
                    Environment.Exit(0)

            | _ -> ()

            return! loop()
        }
    loop()


[<EntryPoint>]
let main argv =

    if (argv.Length <> 3) then printfn "Starting with default values" 
    else 
        topology <-  argv.[1]
        algorithm <- argv.[2]
        if topology = "2D" || topology = "imp2D" || topology = "imp2d" || topology = "2d"  then numNodes <-  argv.[0] |> int |> roundOff2DNodes
        else if topology = "3D" || topology = "imp3D" || topology = "imp3d" || topology = "3d"  then numNodes <-  argv.[0] |> int |> roundOff3DNodes
        else numNodes <- argv.[0] |> int

        if topology = "imp2D" || topology = "imp2d" || topology = "imp3D" || topology = "imp3d" then isImproper <- true
   
    mainActorRef <- spawn gossipSystem "MainActor" MainActor
    mainActorRef <! BuildTopology("start")

    gossipSystem.WhenTerminated.Wait()

    0