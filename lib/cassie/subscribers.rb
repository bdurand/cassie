# Thread safe list of subscribers. Each subscriber must respond to the :call method.
class Cassie::Subscribers
  
  def initialize(parent_subscribers = nil)
    @array = [].freeze
    @lock = Mutex.new
    @parent_subscribers = parent_subscribers
  end
  
  def add(subscriber)
    @lock.synchronize do
      new_array = @array.dup
      new_array << subscriber
      @array = new_array
    end
  end
  alias_method :<<, :add
  
  def remove(subscriber)
    removed = nil
    @lock.synchronize do
      new_array = @array.dup
      removed = new_array.delete(subscriber)
      @array = new_array
    end
    removed
  end
  alias_method :delete, :remove
  
  def clear
    @array = []
  end
  
  def size
    @array.size + (@parent_subscribers ? @parent_subscribers.size : 0)
  end
  
  def empty?
    size == 0
  end
  
  def each(&block)
    @array.each(&block)
    if @parent_subscribers
      @parent_subscribers.each(&block)
    end
  end
  
  def include?(subscriber)
    @array.include?(subscriber)
  end
  
end
