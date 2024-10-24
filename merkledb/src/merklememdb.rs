use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use bincode::Options;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::error::*;
use crate::internal_methods::assign_all_parents;
use crate::merkledb_debug::*;
use crate::merkledb_reconstruction::*;
use crate::merkledbbase::*;
use crate::merklenode::*;

/**
 * Since we want the graph to use small node ID values for connectivity,
 * The database is stored as two parts a NodeDB: which is ID->Node
 * and a hashdb which is Hash->ID
 */

#[derive(Serialize, Deserialize, Clone)]
pub struct MerkleMemDB {
    /// Stores NodeId->MerkleNode
    nodedb: Vec<MerkleNode>,
    /// Stores NodeId->MerkleNodeAttributes
    attributedb: Vec<MerkleNodeAttributes>,
    /// stores NodeHash->NodeId
    #[serde(skip)]
    hashdb: FxHashMap<MerkleHash, MerkleNodeId>,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    changed: bool,
    /// if set (defaults to true) will sync the DB on drop
    #[serde(skip)]
    autosync: bool,
}

impl Default for MerkleMemDB {
    /**
     * Create an empty in-memory only DB
     */
    fn default() -> MerkleMemDB {
        /*
         * We create a node 0 containing the hash of all 0s used to denote
         * the empty string. This is both a CAS and a FILE node for simplicity,
         * to allow for both the empty string FILE and the empty string CAS
         * case without requiring much explicit client-side handling.
         */
        let mut node_0_attributes = MerkleNodeAttributes::default();
        node_0_attributes.set_file();
        node_0_attributes.set_cas();
        let mut ret = MerkleMemDB {
            nodedb: vec![MerkleNode::default(); 1],  // first real node starts at index 1
            attributedb: vec![node_0_attributes; 1], // first real node starts at index 1
            hashdb: FxHashMap::default(),
            path: PathBuf::default(),
            changed: false,
            autosync: true,
        };
        // for the 0 node.
        ret.hashdb.insert(MerkleHash::default(), 0);
        ret
    }
}

impl MerkleMemDB {
    /**
     * Opens the database at a given path, creating it if it does not already
     * exist. Two databases are created. one at [PATH]/node to store the
     * node DB, and another at [PATH]/hash to store the hash DB.
     */
    pub fn open<T: AsRef<Path>>(path: T) -> Result<MerkleMemDB> {
        let f = File::open(path.as_ref());
        debug!("Opening DB {:?}", path.as_ref());
        #[allow(clippy::field_reassign_with_default)]
        if let Ok(f) = f {
            let buf = BufReader::with_capacity(1024 * 1024, f); // 1MB
            let mut memdb = MerkleMemDB::open_reader(buf)?;
            memdb.path = path.as_ref().into();
            Ok(memdb)
        } else {
            let mut memdb = MerkleMemDB::default();
            memdb.path = path.as_ref().into();
            memdb.changed = true;
            Ok(memdb)
        }
    }
    pub fn open_reader<T: Read>(r: T) -> Result<MerkleMemDB> {
        debug!("Opening DB from reader");
        #[allow(clippy::field_reassign_with_default)]
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        let mut memdb: MerkleMemDB = options.deserialize_from(r)?;
        memdb.rebuild_hash();
        memdb.path = PathBuf::default();
        Ok(memdb)
    }

    pub fn write_into<T: Write>(&mut self, writer: T) {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(writer, &self).unwrap();
    }
    pub fn rebuild_hash(&mut self) {
        for n in self.nodedb.iter() {
            self.hashdb.insert(*n.hash(), n.id());
        }
    }
    pub fn assign_from(&mut self, other: &MerkleMemDB) {
        self.nodedb.clone_from(&other.nodedb);
        self.attributedb.clone_from(&other.attributedb);
        self.hashdb.clone_from(&other.hashdb);
    }

    /// Merges the self with another db
    /// union_with can be called repeatedly, but union_finalize has
    /// to be called at the end
    pub fn union_with(&mut self, other: &MerkleMemDB) {
        let mut old_id_to_new_id: FxHashMap<MerkleNodeId, MerkleNodeId> = FxHashMap::default();
        let mut cur_insert_id = self.nodedb.len() as MerkleNodeId;
        // look at all the new keys and determine the IDs we are assigning them to
        for n in &other.nodedb {
            if !self.hashdb.contains_key(n.hash()) {
                old_id_to_new_id.insert(n.id(), cur_insert_id);
                cur_insert_id += 1;
            } else {
                let mynode = self.find_node(n.hash()).unwrap();
                old_id_to_new_id.insert(n.id(), mynode.id());
            }
        }
        for n in &other.nodedb {
            if !self.hashdb.contains_key(n.hash()) {
                // convert children to hash
                // then to local ID.
                let ch = n
                    .children()
                    .iter()
                    .map(|x| (*old_id_to_new_id.get(&(x.0 as MerkleNodeId)).unwrap(), x.1))
                    .collect();
                self.add_node(n.hash(), n.len(), ch);
            } else {
                // It is possible that I have a partial tree.
                // i.e. I do not think a node has children,
                // when it actually does and so I do actually need to update
                // my list of children.
                let id = *old_id_to_new_id.get(&n.id()).unwrap();
                if !n.children().is_empty() && self.nodedb[id as usize].children().is_empty() {
                    let ch = n
                        .children()
                        .iter()
                        .map(|x| (*old_id_to_new_id.get(&(x.0 as MerkleNodeId)).unwrap(), x.1))
                        .collect();
                    self.nodedb[id as usize].set_children(ch);
                }
            }
        }

        for n in &other.nodedb {
            let newid = *old_id_to_new_id.get(&n.id()).unwrap();
            let attr = other.attributedb[n.id() as usize];
            let selfattr = &mut self.attributedb[newid as usize];
            selfattr.merge_attributes(&attr);
        }
    }
    /// This function must be called after merges
    /// We recompute all the parent relationships
    pub fn union_finalize(&mut self) -> Result<()> {
        if self.nodedb.len() != self.attributedb.len() {
            return Err(MerkleDBError::GraphInvariantError("NodeDB and AttributeDB length mismatch".into()));
        }
        for nodetype in [NodeDataType::CAS, NodeDataType::FILE] {
            let nodelist: Vec<MerkleNodeId> = self
                .nodedb
                .iter()
                .filter_map(|x| {
                    if self.attributedb[x.id() as usize].is_type(nodetype) {
                        Some(x.id())
                    } else {
                        None
                    }
                })
                .collect();
            assign_all_parents(self, nodelist, nodetype);
        }

        Ok(())
    }

    /// Computes the set difference between a and b and add it to self.
    /// effectively self = a - b
    pub fn difference(&mut self, a: &MerkleMemDB, b: &MerkleMemDB) {
        let mut a_id_to_new_id: FxHashMap<MerkleNodeId, MerkleNodeId> = FxHashMap::default();
        // we loop through a looking for it in b
        for n in &a.nodedb {
            let a_attr = a.attributedb[n.id() as usize];
            // if b does not contain this node
            // OR b's node and a's node have different attributes,
            // Then I need to add.
            if !b.hashdb.contains_key(n.hash()) || !b.attributedb[b.hashdb[n.hash()] as usize].type_equal(&a_attr) {
                // b does not contain this node. so this is part
                // of the set difference. All the nodes in a - b
                // must be *complete* (i.e. have children information)

                // look for it in self
                if let Some(selfid) = self.hashdb.get(n.hash()) {
                    // self already has it, so skip. Cache the lookup
                    a_id_to_new_id.insert(n.id(), *selfid);
                } else {
                    // ok we need to do an insert.
                    let newn = self.add_node(n.hash(), n.len(), Vec::new());
                    a_id_to_new_id.insert(n.id(), newn.id());
                }
                // loop through n's children and make sure they exist,
                // inserting an empty children list if they do not.
                for ch in n.children() {
                    let chnode = a.find_node_by_id(ch.0).unwrap();
                    let newnode = self.add_node(chnode.hash(), chnode.len(), Vec::new());
                    a_id_to_new_id.insert(ch.0, newnode.id());
                }
            }
        }
        // loop through one more time updating the children information
        // and also updating the parent information
        for n in &a.nodedb {
            if !b.hashdb.contains_key(n.hash()) {
                // this is an all new node. we have to create all the children
                // information
                let id = *a_id_to_new_id.get(&n.id()).unwrap();
                if !n.children().is_empty() && self.nodedb[id as usize].children().is_empty() {
                    let newch = n
                        .children()
                        .iter()
                        .map(|x| (*a_id_to_new_id.get(&(x.0 as MerkleNodeId)).unwrap(), x.1))
                        .collect();
                    self.nodedb[id as usize].set_children(newch);
                }
            }
            // we carry all the attribute data always
            if let Some(id) = a_id_to_new_id.get(&n.id()) {
                let attr = a.attributedb[n.id() as usize];
                let selfattr = &mut self.attributedb[*id as usize];
                if attr.is_file() {
                    selfattr.set_file();
                }
                if attr.is_cas() {
                    selfattr.set_cas();
                }
            }
        }
    }
    pub fn print_node(&self, n: &MerkleNode) -> String {
        // make a list of children hashes , substituting a "?" for every
        // children that cannot be found
        let chlist: Vec<_> = n
            .children()
            .iter()
            .map(|id| {
                self.find_node_by_id(id.0)
                    .map_or("?".to_string(), |node| node.hash().to_string())
            })
            .collect();

        // print the attribute string substituting a "?" if the attribute cannot
        // be found
        let attr_string = self
            .attributedb
            .get(n.id() as usize)
            .map_or("?".to_string(), |attr| format!("{attr:?}"));
        format!("{}: len:{} children:{:?} attr:{:?}", n.hash(), n.len(), chlist, attr_string)
    }
    pub fn get_path(&self) -> &Path {
        &self.path
    }

    pub fn node_iterator(&self) -> impl Iterator<Item = &MerkleNode> {
        self.nodedb.iter()
    }

    pub fn attr_iterator(&self) -> impl Iterator<Item = &MerkleNodeAttributes> {
        self.attributedb.iter()
    }

    pub fn is_empty(&self) -> bool {
        // the default db has first real node starts at index 1
        self.nodedb.len() == 1
    }
}

impl Drop for MerkleMemDB {
    fn drop(&mut self) {
        if self.autosync {
            self.flush().unwrap();
        }
    }
}
impl Debug for MerkleMemDB {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for n in &self.nodedb {
            writeln!(f, "{}", self.print_node(n))?;
        }
        Ok(())
    }
}

// Direct database interaction routines
impl MerkleDBBase for MerkleMemDB {
    /**
     * Creates a node with the given specification,
     * returning the MerkleNode object.
     *
     * This should be used anytime
     * a new MerkleNode object is needed since the MerkleNode also stores its
     * current ID, and that cannot be derived without actually performing
     * a database insertion (and incrementing next_node_id).
     * The hashdb index also needs to be updated as well.
     */
    fn maybe_add_node(
        &mut self,
        hash: &MerkleHash,
        len: usize,
        children: Vec<(MerkleNodeId, usize)>,
    ) -> (MerkleNode, bool) {
        if let Some(node) = self.find_node(hash) {
            (node, false)
        } else {
            self.changed = true;
            let id = self.nodedb.len() as MerkleNodeId;
            let node = MerkleNode::new(id, *hash, len, children);
            self.hashdb.insert(*hash, id);
            self.nodedb.push(node.clone());
            self.attributedb.push(MerkleNodeAttributes::default());
            (node, true)
        }
    }

    /**
     * Find a node by ID, returning None if such a node does not exist.
     * Panics if the DB cannot be read.
     */
    fn find_node_by_id(&self, h: MerkleNodeId) -> Option<MerkleNode> {
        if (h as usize) < self.nodedb.len() {
            let ret = &self.nodedb[h as usize];
            Some(ret.clone())
        } else {
            None
        }
    }
    /**
     * Converts a Hash to an ID returning None if such a hash does not exist.
     * Panics if the DB cannot be read.
     */
    fn hash_to_id(&self, h: &MerkleHash) -> Option<MerkleNodeId> {
        self.hashdb.get(h).copied()
    }

    /**
     * Find a node by Hash, returning None if such a node does not exist.
     * Panics if the DB cannot be read.
     */
    fn find_node(&self, h: &MerkleHash) -> Option<MerkleNode> {
        self.hash_to_id(h).and_then(|x| self.find_node_by_id(x))
    }
    fn node_attributes(&self, h: MerkleNodeId) -> Option<MerkleNodeAttributes> {
        self.attributedb.get(h as usize).cloned()
    }

    fn set_node_attributes(&mut self, h: MerkleNodeId, attr: &MerkleNodeAttributes) -> Option<()> {
        let index = h as usize;
        if index < self.attributedb.len() {
            self.changed = true;
            self.attributedb[index] = *attr;
            Some(())
        } else {
            None
        }
    }
    fn flush(&mut self) -> Result<()> {
        if self.changed && !self.path.as_os_str().is_empty() {
            use std::io::{Error, ErrorKind};
            let dbpath = self.path.parent().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Unable to find MerkleDB output parent path from {:?}", self.path),
                )
            })?;
            // we prefix with "[PID]." for now. We should be able to do a cleanup
            // in the future.
            let tempfile = tempfile::Builder::new()
                .prefix(&format!("{}.", std::process::id()))
                .suffix(".db")
                .tempfile_in(dbpath)?;
            debug!("Flushing DB to {:?} via {:?}", self.path, tempfile.path());
            let f = BufWriter::new(&tempfile);
            self.write_into(f);
            self.changed = false;
            // e is a PersistError. e.error is an IoError
            tempfile.persist(&self.path).map_err(|e| e.error)?;
        }
        Ok(())
    }
    fn get_sequence_number(&self) -> MerkleNodeId {
        self.nodedb.len() as MerkleNodeId
    }

    fn db_invariant_checks(&self) -> bool {
        let ret = self.hashdb.len() == self.attributedb.len() && self.attributedb.len() == self.nodedb.len();
        if !ret {
            error!(
                "DB length mismatch: HashDB len {} , attributeDB len {} nodedb len {}",
                self.hashdb.len(),
                self.attributedb.len(),
                self.nodedb.len()
            );
        }
        ret
    }
    fn autosync_on_drop(&mut self, autosync: bool) {
        self.autosync = autosync;
    }
}

impl MerkleDBReconstruction for MerkleMemDB {}
impl MerkleDBDebugMethods for MerkleMemDB {}
