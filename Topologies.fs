module TopologyGenerators

open System
open Akka.Actor
open Akka.FSharp

// Full Network Topology
// All actors are neighbors  to all other actors.
let findFullNetworkFor (nodes:list<IActorRef>, index:int, numofNodes:int) =
    let arrayNeighbor = nodes |> List.indexed |> List.filter (fun (i, _) -> i <> index) |> List.map snd
    arrayNeighbor


// Line Topology
// Linear topology
let generateLinear (nodes: IActorRef list , index:int) =
    let mutable arrayNeighbor = []
    if index > 0 then 
        arrayNeighbor <- nodes.[index-1] :: arrayNeighbor  
    if index < nodes.Length - 1 then
        arrayNeighbor <- nodes.[index+1] :: arrayNeighbor
    arrayNeighbor


// 2D Grid Topology
// Simple square topology. Max 4 neighbours possible - X-axis : left & right | Y-axis : top & bottom.
let generate2DGrid (nodes:list<IActorRef>, index:int, side:int, isImproper:bool) =
    let mutable arrayNeighbor = []
    // Add horizontal neighbors
    if index % side <> 0 then
       arrayNeighbor <- nodes.[index-1] :: arrayNeighbor
    if index % side <> side - 1 then
        arrayNeighbor <- nodes.[index+1] :: arrayNeighbor 
    // Add vertical neighbors
    if index - side >= 0 then 
        arrayNeighbor <- nodes.[index-side] :: arrayNeighbor
    if (index + side) < nodes.Length then 
        arrayNeighbor <- nodes.[index+side] :: arrayNeighbor

    if isImproper then
        let r = System.Random()
        let randomNeighbour =
            nodes
            |> List.filter (fun x -> (x <> nodes.[index] && not (List.contains x arrayNeighbor)))
            |> fun y -> y.[r.Next(y.Length - 1)]
        arrayNeighbor <- randomNeighbour :: arrayNeighbor

    arrayNeighbor


// 3D Grid Topology
// Simple cube topology. Max 6 neighbours possible - X-axis : left & right | Y-axis : top & bottom | Z-axis : front & back
let generate3DGrid (nodes:list<IActorRef>, index:int, side:int, layerSize:int, numNodes:int, isImproper:bool) =
    let mutable arrayNeighbor = []
    // Adding Horizontal neighbors
    if (index % side) > 0 then
        arrayNeighbor <- nodes.[index-1] :: arrayNeighbor 
    if (index % side) < (side - 1) then
        arrayNeighbor <- nodes.[index+1] :: arrayNeighbor 
    // Adding Vertical neighbors
    if index % layerSize >= side then 
        arrayNeighbor <- nodes.[index-side] :: arrayNeighbor
    if layerSize - (index % layerSize) > side then 
        arrayNeighbor <- nodes.[index+side] :: arrayNeighbor
    // Adding depth neighbors
    if index >= layerSize then 
        arrayNeighbor <- nodes.[index-layerSize] :: arrayNeighbor
    if (numNodes - index) > layerSize then 
        arrayNeighbor <- nodes.[index+layerSize] :: arrayNeighbor

    // in case topology is incorrect, adding one random node to the regular neighbors. 
    if isImproper then
        let r = System.Random()
        let randomNeighbour =
            nodes
            |> List.filter (fun x -> (x <> nodes.[index] && not (List.contains x arrayNeighbor)))
            |> fun y -> y.[r.Next(y.Length - 1)]
        arrayNeighbor <- randomNeighbour :: arrayNeighbor

    arrayNeighbor