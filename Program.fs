module Main

open Akka
open System
open TopologyGenerators
open Schedulers
open Akka.Actor
open Akka.Actor.Scheduler
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
open Akka.Configuration

// Set algo parameters here
let push_sum_convergence_criteria = 15
let push_sum_rounds_time_ms = 50.0

let gossipSystem = ActorSystem.Create("GossipSystem")
let mutable actor_ref = null

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
let round_of_3d_nodes (numNodes:int) =
    let sides = numNodes |> float |> Math.Cbrt |> ceil |> int
    pown sides 3

// For 2D grid, rounding off the total nodes to next nearest square
let round_of_2d_nodes (numNodes:int) =
    let sides = numNodes |> float |> sqrt |> ceil |> int
    pown sides 2

// Each actor (node) will call this function to maintain its own list of neighbours

//TBC
let find_neighbours (pool:list<IActorRef>, topology:string, myActorIndex:int) = 
    let mutable neighbours = []
    match topology with 
    | "line" ->
        neighbours <- TopologyGenerators.generateLinear(pool, myActorIndex)
    | "2D" | "2d" | "imp2D" | "imp2d" ->
        let side = numNodes |> float |> sqrt |> int
        neighbours <- TopologyGenerators.generate2DGrid(pool, myActorIndex, side, isImproper)
    | "3D" | "3d" | "imp3D" | "imp3d" ->
        let side = numNodes |> float |> Math.Cbrt |> int
        neighbours <- TopologyGenerators.generate3DGrid(pool, myActorIndex, side, (side * side), numNodes, isImproper)
    | "full" ->
        neighbours <- TopologyGenerators.findFullNetworkFor(pool, myActorIndex, numNodes)
    | _ -> ()
    neighbours


let Gossip (id:int) (mailbox:Actor<_>) =
    let mutable neighbours_array = []
    let mutable rumour_count = 0
    let myActorIndex = id
    let mutable is_active = 1
 
    let rec loop () = actor {
        if is_active = 1 then
            let! (message) = mailbox.Receive()

            match message with 
            | FindNeighbours(pool, topology) ->
                neighbours_array <- find_neighbours(pool, topology, myActorIndex)
                actor_ref <! IFoundNeighbours(myActorIndex)

            | GossipMessage(fromNode, message) ->
                if rumour_count = 0 then
                    actor_ref <! IReceivedRumour(myActorIndex)
                    mailbox.Self <! Round

                rumour_count <- rumour_count + 1

                if rumour_count > 10 then
                    is_active <- 0
                    actor_ref <! GossipActorStoppedTransmitting(mailbox.Self.Path.Name)

            | Round(_) ->
                // Find a random neighbour form neighbour list
                let randomNeighbour = Random().Next(neighbours_array.Length)
                let randomActor = neighbours_array.[randomNeighbour]
                // Send GossipMessage to it
                randomActor <! GossipMessage(myActorIndex, "rumour")
                gossipSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self, Round, mailbox.Self)
                
            | _ -> ()

            return! loop()
        }
    loop()


let PushSum (myActorIndex:int) (mailbox:Actor<_>) =
    let mutable neighbours_array = []
    let mutable s = myActorIndex + 1 |> float
    let mutable w = 1 |> float
    let mutable prev_ratio = s/w
    let mutable curr_ratio = s/w
    let mutable towards_end = 0
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
                neighbours_array <- find_neighbours(pool, topology, myActorIndex)
                actor_ref <! IFoundNeighbours(myActorIndex)

            | Round(_) ->
                
                // Step 2: s <- Σ(incomingsList) and w <- Σ(incomingwList)
                s <- s_aggregateTminus1
                w <- w_aggregateTminus1

                // Step 3: Choose a random neighbour
                let randomNeighbour = Random().Next(0, neighbours_array.Length)
                let randomActor = neighbours_array.[randomNeighbour]

                // Step 4: Send the pair ( ½s , ½w ) to randomNeighbour and self
                randomActor <! PushsumMessage(myActorIndex , (s/2.0) , (w/2.0))
                mailbox.Self <! PushsumMessage(myActorIndex , (s/2.0) , (w/2.0))

                // Check for convergence
                curr_ratio <- s / w
                if (abs(curr_ratio - prev_ratio)) < (pown 10.0 -10) then 
                    towards_end <- towards_end + 1
                else 
                    towards_end <- 0
                
                if towards_end = push_sum_convergence_criteria then 
                    actor_ref <! PushsumActorConverged(myActorIndex, s,w)
                    isActive <- 0

                prev_ratio <- curr_ratio

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
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(push_sum_rounds_time_ms), mailbox.Self, Round)
 
            | _ -> ()

            return! loop()
        }
    loop()

let MainActor (mailbox:Actor<_>) =    
    let mutable actors_done = 0
    let mutable actors_that_know = 0
    let mutable topology_built = 0

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | BuildTopology(_) ->
                printfn "BuildTopology"
                if algorithm = "gossip" then    
                    actorsPool <-
                        [0 .. numNodes-1]
                        |> List.map(fun id -> spawn gossipSystem (sprintf "GossipActor_%d" id) (Gossip id))
                else
                    actorsPool <- 
                        [0 .. numNodes-1]
                        |> List.map(fun id -> spawn gossipSystem (sprintf "PushsumActor_%d" id) (PushSum id))

                actorsPool |> List.iter (fun item -> 
                    item <! FindNeighbours(actorsPool, topology))

            | IFoundNeighbours(index) ->
                topology_built <- topology_built + 1
                if topology_built = numNodes then 
                    printfn "Topology completely built! \n"
                    timer.Start()
                    actor_ref <! StartAlgorithm(algorithm)

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
                actors_done <- actors_done + 1

            | IReceivedRumour(actorIndex) ->
                actors_that_know <- actors_that_know + 1
                printfn "%d knows! Total = %d\n" actorIndex actors_that_know
                if actors_that_know = numNodes then 
                    timer.Stop()
                    printfn "\nTotal time = %d ms | Total Terminated = %d\n" timer.ElapsedMilliseconds actors_done
                    printfn "\n\n Everyone knows the rumour!!\n"
                    Environment.Exit(0)

            | PushsumActorConverged (index, s, w) ->
                actors_done <- actors_done + 1
                printfn "id = %d | s = %f | w = %f | s/w = %f | Total terminated = %d"  index s w (s/w) actors_done
                if actors_done = numNodes then 
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
        if topology = "2D" || topology = "imp2D" || topology = "imp2d" || topology = "2d"  then numNodes <-  argv.[0] |> int |> round_of_2d_nodes
        else if topology = "3D" || topology = "imp3D" || topology = "imp3d" || topology = "3d"  then numNodes <-  argv.[0] |> int |> round_of_3d_nodes
        else numNodes <- argv.[0] |> int

        if topology = "imp2D" || topology = "imp2d" || topology = "imp3D" || topology = "imp3d" then isImproper <- true
   
    actor_ref <- spawn gossipSystem "MainActor" MainActor
    actor_ref <! BuildTopology("start")

    gossipSystem.WhenTerminated.Wait()

    0
