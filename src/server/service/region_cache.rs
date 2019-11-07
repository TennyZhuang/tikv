use kvproto::{errorpb, kvrpcpb, metapb};
use std::collections::{BTreeMap, HashMap};

pub type Key = Vec<u8>;

#[derive(Clone)]
pub struct Region {
    region: metapb::Region,
    leader: Option<metapb::Peer>,
}

impl Region {
    pub fn contains(&self, key: &[u8]) -> bool {
        let start_key = self.region.get_start_key();
        let end_key = self.region.get_end_key();
        key >= start_key && (key < end_key || end_key.is_empty())
    }

    #[inline]
    pub fn epoch(&self) -> &metapb::RegionEpoch {
        self.region.get_region_epoch()
    }

    #[inline]
    pub fn start_key(&self) -> Key {
        self.region.get_start_key().to_vec().into()
    }

    #[inline]
    pub fn end_key(&self) -> Key {
        self.region.get_end_key().to_vec().into()
    }

    #[inline]
    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.region.get_id()
    }

    // Return store id
    #[inline]
    pub fn get_store_id(&self) -> Option<u64> {
        self.leader.as_ref().and_then(|s| Some(s.get_store_id()))
    }
}

#[derive(Default, Clone)]
pub struct RegionCache {
    // start_key => Region
    regions: BTreeMap<Key, Region>,
    // region id => start_key
    id_to_start_key: HashMap<u64, Key>,
}

impl RegionCache {
    // Returns RPCContext for a region by a certain key
    // Returns `None` if no region found by `region_id` or the region does not have a leader peer
    pub fn generate_rpc_context(&self, key: &Key) -> Option<kvrpcpb::Context> {
        self.find_region_by_key(key).and_then(|r| {
            r.leader.as_ref().and_then(|leader| {
                let mut ctx = kvrpcpb::Context::default();
                ctx.set_region_id(r.region.id);
                ctx.set_region_epoch(r.epoch().clone());
                ctx.set_peer(leader.clone());
                Some(ctx)
            })
        })
        // TODO: remove region with empty leader peer automatically?
    }

    /// Returns the region which contains the given key in the region cache.
    /// Returns `None` if no region in the cache contains the key.
    pub fn find_region_by_key(&self, key: &Key) -> Option<&Region> {
        let (_, candidate_region) = self.regions.range::<Key, _>(..=key).next_back()?;
        if candidate_region.contains(key) {
            Some(candidate_region)
        } else {
            None
        }
    }

    /// Add a region to the cache. It removes intersecting regions in the cache automatically.
    pub fn add_region(&mut self, region: Region) {
        let end_key = region.end_key();
        let mut to_be_removed = Vec::new();
        let mut search_range = if end_key.is_empty() {
            self.regions.range::<Key, _>(..)
        } else {
            self.regions.range::<Key, _>(..end_key)
        };
        // Remove intersecting regions with end_key in (new.start_key, new.end_key)
        while let Some((_, region_in_cache)) = search_range.next_back() {
            if region_in_cache.end_key() > region.start_key() {
                to_be_removed.push(region_in_cache.id());
            } else {
                break;
            }
        }
        for id in to_be_removed {
            let start_key = match self.id_to_start_key.remove(&id) {
                Some(id) => id,
                None => {
                    // Cache must be corrupt, give up and start again.
                    self.clear();
                    break;
                }
            };
            self.regions.remove(&start_key);
        }
        self.id_to_start_key.insert(region.id(), region.start_key());
        self.regions.insert(region.start_key(), region);
    }

    #[inline]
    pub fn find_region_by_id(&self, id: u64) -> Option<&Region> {
        let start_key = self.id_to_start_key.get(&id)?;
        self.regions.get(start_key)
    }

    #[inline]
    fn remove_region_by_id(&mut self, id: u64) {
        if let Some(start_key) = self.id_to_start_key.remove(&id) {
            self.regions.remove(&start_key);
        }
    }

    pub fn on_region_error(&mut self, error: &errorpb::Error) {
        if let Some(not_leader) = error.not_leader.as_ref() {
            if let Some(start_key) = self.id_to_start_key.get(&not_leader.get_region_id()) {
                // Update region leader
                if let Some(region) = self.regions.get_mut(start_key) {
                    // TODO: if not_leader.leader is None, choose another peer instead ?
                    region.leader = not_leader.clone().leader.into_option()
                }
            }
        }
        if let Some(region_not_found) = error.region_not_found.as_ref() {
            self.remove_region_by_id(region_not_found.get_region_id())
        }
        if let Some(key_not_in_region) = error.key_not_in_region.as_ref() {
            self.remove_region_by_id(key_not_in_region.get_region_id())
        }
        if let Some(epoch_not_match) = error.epoch_not_match.as_ref() {
            // TODO
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.regions.clear();
        self.id_to_start_key.clear();
    }
}
