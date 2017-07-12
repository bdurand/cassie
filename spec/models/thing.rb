class Cassie::Thing
  include Cassie::Model
  
  self.table_name = "things"
  self.keyspace = "test"
  self.primary_key = [:owner, :id]
  self.read_consistency = :one
  self.write_consistency = :quorum
  
  column :owner, :int
  column :id, :int, :as => :identifier
  column :val, :varchar, :as => :value
  
  ordering_key :id, :desc
  
  validates_presence_of :owner, :id
  
  before_save do
    callbacks << :save
  end
  
  before_create do
    callbacks << :create
  end
  
  before_update do
    callbacks << :update
  end
  
  before_destroy do
    callbacks << :destroy
  end
  
  def callbacks
    @callbacks ||= []
  end
end
