package model

import "github.com/restic/restic/internal/restic"

type ID = restic.ID
type IDs = restic.IDs
type Node = restic.Node
type NodeType = restic.NodeType

var (
	NodeTypeFile      = restic.NodeTypeFile
	NodeTypeDir       = restic.NodeTypeDir
	NodeTypeSymlink   = restic.NodeTypeSymlink
	NodeTypeDev       = restic.NodeTypeDev
	NodeTypeCharDev   = restic.NodeTypeCharDev
	NodeTypeFifo      = restic.NodeTypeFifo
	NodeTypeSocket    = restic.NodeTypeSocket
	NodeTypeIrregular = restic.NodeTypeIrregular
	NodeTypeInvalid   = restic.NodeTypeInvalid
)
