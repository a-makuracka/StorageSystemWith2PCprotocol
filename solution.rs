use async_channel::Sender;
use executor::{Handler, ModuleRef};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;


#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub(crate) enum ProductType {
    Electronics,
    Toys,
    Books,
}

#[derive(Clone)]
pub(crate) struct StoreMsg {
    sender: ModuleRef<CyberStore2047>,
    content: StoreMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum StoreMsgContent {
    /// Transaction Manager initiates voting for the transaction.
    RequestVote(Transaction),
    /// If every process is ok with transaction, TM issues commit.
    Commit,
    /// System-wide abort.
    Abort,
}

#[derive(Clone)]
pub(crate) struct NodeMsg {
    content: NodeMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum NodeMsgContent {
    /// Process replies to TM whether it can/cannot commit the transaction.
    RequestVoteResponse(TwoPhaseResult),
    /// Process acknowledges to TM committing/aborting the transaction.
    FinalizationAck,
}

pub(crate) struct TransactionMessage {
    /// Request to change price.
    pub(crate) transaction: Transaction,

    /// Called after 2PC completes (i.e., the transaction was decided to be
    /// committed/aborted by CyberStore2047). This must be called after responses
    /// from all processes acknowledging commit or abort are collected.
    pub(crate) completed_callback:
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TwoPhaseResult {
    Ok,
    Abort,
}

#[derive(Copy, Clone)]
pub(crate) struct Product {
    pub(crate) identifier: Uuid,
    pub(crate) pr_type: ProductType,
    pub(crate) price: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) pr_type: ProductType,
    pub(crate) shift: i32,
}

pub(crate) struct ProductPriceQuery {
    pub(crate) product_ident: Uuid,
    pub(crate) result_sender: Sender<ProductPrice>,
}

pub(crate) struct ProductPrice(pub(crate) Option<u64>);

/// Message which disables a node. Used for testing.
pub(crate) struct Disable;

/// CyberStore2047.
/// This structure serves as TM.
pub(crate) struct CyberStore2047 {
    nodes: Vec<ModuleRef<Node>>,
    nodes_count: u64,
    replies_from_nodes_count: u64,
    ack_count: u64,
    was_abort: bool,
    current_transaction_msg: Option<TransactionMessage>,
}

impl CyberStore2047 {
    pub(crate) fn new(nodes: Vec<ModuleRef<Node>>) -> Self {
        CyberStore2047 { 
            nodes: nodes.clone(),
            nodes_count: nodes.len() as u64,
            replies_from_nodes_count: 0, 
            ack_count: 0,
            was_abort: false,
            current_transaction_msg: None,
        }
    }
}

/// Node of CyberStore2047.
/// This structure serves as a process of the distributed system.
pub(crate) struct Node {
    products: Vec<Product>,
    pending_transaction: Option<Transaction>,
    enabled: bool,
}

impl Node {
    pub(crate) fn new(products: Vec<Product>) -> Self {
        Self {
            products,
            pending_transaction: None,
            enabled: true,
        }
    }

    pub(crate) fn get_product_price(&self, product_ident: Uuid) -> Option<ProductPrice> {
        self.products
            .iter()
            .find(|product| product.identifier == product_ident)
            .map(|product| ProductPrice(Some(product.price)))
    }

    pub(crate) fn check_can_commit(&self, transaction: &Transaction) -> bool {
        let mut can_commit = true;
        for product in &self.products {
            if product.pr_type == transaction.pr_type {
                if (product.price as i64) + (transaction.shift as i64) <= 0 {
                    can_commit = false;
                    break;
                }
            }
        }
        can_commit
    }

    pub(crate) fn commit_transaction(&mut self, transaction: Option<Transaction>) {
        if let Some(transaction) = transaction {
            for product in &mut self.products {
                if product.pr_type == transaction.pr_type {
                    let new_price = (product.price as i64) + (transaction.shift as i64);
                    product.price = new_price as u64;        
                }
            }
        }
    }   
}

#[async_trait::async_trait]
impl Handler<NodeMsg> for CyberStore2047 {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: NodeMsg) {
        match msg.content {
            NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Ok) => {
                self.replies_from_nodes_count += 1;
                // Check if all nodes have already confirmed and none of them have replied with 'abort'. 
                if self.replies_from_nodes_count == self.nodes_count && !self.was_abort {
                    // Create commit message.
                    let commit_msg = StoreMsg {
                        sender: self_ref.clone(),
                        content: StoreMsgContent::Commit,
                    };
                    // Send commit message to all nodes.
                    for node in &self.nodes {
                        let _ = node.send(commit_msg.clone()).await;
                    }
                } 
            }
            NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Abort) => {
                self.replies_from_nodes_count += 1;
                // Check if this is first 'abort' for the current transaction.
                if !self.was_abort { 
                    self.was_abort = true;
                    // Create abort message.
                    let abort_msg = StoreMsg {
                        sender: self_ref.clone(),
                        content: StoreMsgContent::Abort,
                    };
                    // Send abort message to all nodes.
                    for node in &self.nodes {
                        let _ = node.send(abort_msg.clone()).await;
                    }
                }
            }
            NodeMsgContent::FinalizationAck => {
                self.ack_count += 1;   
                // Check if all nodes have already sent FinalizationAck.                 
                if self.ack_count == self.nodes_count {
                    if let Some(mut transaction_msg) = self.current_transaction_msg.take() {
                        let callback = std::mem::replace(
                            &mut transaction_msg.completed_callback,
                            Box::new(|_| Box::pin(async {})), 
                        );
                        let mut result = TwoPhaseResult::Ok;
                        if self.was_abort {
                            result = TwoPhaseResult::Abort;
                        } 
                        let future = callback(result);
                        let _ = future.await;

                        // Restore the value of those fields
                        // because this is the last handler execution for this transaction.
                        self.replies_from_nodes_count = 0;
                        self.ack_count = 0;
                        self.was_abort = false;
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<StoreMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: StoreMsg) {
        if self.enabled {      
            match msg.content {
                StoreMsgContent::RequestVote(ref transaction) => {
                    if self.check_can_commit(transaction) {
                        self.pending_transaction = Some(*transaction);
                        let commit_msg = NodeMsg {
                            content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Ok),
                        };
                        let _ = msg.sender.send(commit_msg).await;
                    } else {
                        let abort_msg = NodeMsg {
                            content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Abort),
                        };
                        let _ = msg.sender.send(abort_msg).await;
                    }
                }
                StoreMsgContent::Commit => {
                    self.commit_transaction(self.pending_transaction.clone());
                    let ack_msg = NodeMsg {
                        content: NodeMsgContent::FinalizationAck
                    };
                    let _ = msg.sender.send(ack_msg).await;
                }
                StoreMsgContent::Abort => {
                    let ack_msg = NodeMsg {
                        content: NodeMsgContent::FinalizationAck
                    };
                    let _ = msg.sender.send(ack_msg).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ProductPriceQuery> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ProductPriceQuery) {
        if self.enabled {
            // Check product price.
            if let Some(product_price) = self.get_product_price(msg.product_ident) {
                // Price has been found, send it. 
                match msg.result_sender.send(product_price).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Failed to send product price: {:?}", err);
                    }
                }
            } else {
                // Price has not been found.
                eprintln!("Product with identifier {:?} not found", msg.product_ident);
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Disable) {
        self.enabled = false;
    }
}

#[async_trait::async_trait]
impl Handler<TransactionMessage> for CyberStore2047 {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: TransactionMessage) {
        let transaction = msg.transaction.clone();

        self.current_transaction_msg = Some(msg);

         // Send transaction to all nodes in self.nodes
         for node in &self.nodes {
            let store_msg = StoreMsg {
                sender: self_ref.clone(),
                content: StoreMsgContent::RequestVote(transaction.clone()),
            };
            let _ = node.send(store_msg).await;
        }
    }
}
