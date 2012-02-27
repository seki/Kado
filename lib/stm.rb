require 'drip'
require 'thread'

class STM
  class VersionMismatch < RuntimeError; end
  class TransactionNotFound < RuntimeError; end

  class Ref
    def initialize(stm, name)
      @stm = stm
      @name = name
    end

    def deref
      _deref[1]
    end

    def refset(value)
      context = @stm.context
      raise TransactionNotFound unless context
      context.refset(@name, value)
    end

    def _deref
      context = @stm.context || @stm
      context.deref(@name)
    end
    
    def _age
      _deref[0]
    end

    def newest?(*src)
      my_age = _age
      src.each do |ref|
        return false if my_age < ref._age
      end
      true
    end
  end
  
  class Context
    def initialize(stm)
      @stm = stm
      @deref = {}
      @refset = {}
    end
    
    def deref(name)
      @refset.fetch(name) do
        @deref.fetch(name) do
          @deref[name] = @stm.deref(name)
        end
      end
    end

    def refset(name, value)
      @refset[name] = value
    end

    def commit
      @deref.each do |name, tuple|
        raise VersionMismatch unless tuple == @stm.deref(name)
      end
      @refset.each do |name, value|
        @stm.refset(name, value)
      end
      true
    end
  end
  
  def initialize
    @drip = Drip.new(nil)
    @mutex = Mutex.new
  end

  def ref(value)
    key = @drip.write(:ref)
    name = key.to_s(36)
    @drip.write(value, name)
    Ref.new(self, name)
  end

  def deref(name)
    @drip.head(1, name)[0]
  end

  def refset(name, value)
    @drip.write(value, name)
  end

  def transaction(alter=false, &blk)
    return yield() if context
    do_sync(alter, &blk)
  end

  def context
    Thread.current[:STM]
  end

  private
  def do_sync(alter=false)
    Thread.current[:STM] = Context.new(self)
    yield
    @mutex.synchronize do
      context.commit
    end
  rescue VersionMismatch
    if alter
      retry
    else
      raise $!
    end
  ensure
    Thread.current[:STM] = nil
  end
end

if __FILE__ == $0
  stm = STM.new
  a = stm.ref(0)
  b = stm.ref(0)
  c = stm.ref(0)

  p c.newest?(a, b)
  p b.newest?(a, c)

  t = (1..10).collect do
    Thread.new do
      10.times do
        stm.transaction(true) do
          it = a.deref
          sleep(0.2 * rand)
          a.refset(it + 1)
        end
      end
    end
  end
  
  t.each {|th| th.join}

  p a.newest?(b, c)
  p a.deref


  p [1, b.newest?(a)]
  unless b.newest?(a)
    stm.transaction do
      b.refset(b.deref + 1)
      p [2, b.newest?(a)]
    end
  end
  p [3, b.newest?(a)]
end
