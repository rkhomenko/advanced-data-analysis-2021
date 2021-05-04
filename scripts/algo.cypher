-- Add labels by node type
match (dis:Vertex {type: "Disease"})
match (drug:Vertex {type: "Drug"})
set dis:Disease
set drug:Drug
return dis, drug

-- Algo
match (v:Vertex)
where v.id in [0, 1, 7]
call apoc.path.expandConfig(v, {
	relationshipFilter: "EDGE",
    labelFilter: "+Drug",
    minLevel: 2,
    maxLevel: 2
})
yield path
with nodes(path) as nodes
unwind nodes as n
with n
where n.type = "Drug"
return n
