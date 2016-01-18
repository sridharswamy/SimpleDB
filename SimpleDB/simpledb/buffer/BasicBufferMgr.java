package simpledb.buffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	//TASK 1: Using Map Data Structure to keep track of the buffer pool.
	private HashMap<Block,Buffer> bufferPoolMap;

	public HashMap<Block, Buffer> getBufferPoolMap() 
	{
		return bufferPoolMap;
	}

	/**
	 * Creates a buffer manager having the specified number 
	 * of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects 
	 * that it gets from the class
	 * {@link simpledb.server.SimpleDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until 
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
	 * is called first.
	 * @param numbuffs the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
		bufferPoolMap=new HashMap<Block,Buffer>();
	}

	BasicBufferMgr(){

	}
	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. 
	 * If there is already a buffer assigned to that block
	 * then that buffer is used;  
	 * otherwise, an unpinned buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk block
	 * @return the pinned buffer
	 */
	//TASK 1: Using Map Data Structure to keep track of the buffer pool.
	synchronized Buffer pin(Block blk) 
	{
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) 
		{
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			if(buff.block()!=null)
			{
				bufferPoolMap.remove(buff.block());
			}
			buff.assignToBlock(blk);
			bufferPoolMap.put(blk, buff);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and
	 * pins a buffer to it. 
	 * Returns null (without allocating the block) if 
	 * there are no available buffers.
	 * @param filename the name of the file
	 * @param fmtr a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	//TASK 1: Using Map Data Structure to keep track of the buffer pool.
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		if(buff.block()!=null)
			bufferPoolMap.remove(buff.block());
		buff.assignToNew(filename, fmtr);
		bufferPoolMap.put(buff.block(), buff);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * @param buff the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	//TASK 1: Using Map Data Structure to keep track of the buffer pool.
	private Buffer findExistingBuffer(Block blk) {

		if(bufferPoolMap.containsKey(blk))
		{

			return bufferPoolMap.get(blk);
		}
		return null;
	}
	
	//TASK 2: Implementing LRM.
	private Buffer chooseUnpinnedBuffer()
	{
		if(bufferPoolMap.size()==0)
		{
			for (Buffer buff : bufferpool)
				if (!buff.isPinned())
					return buff;
		}
		else
		{
			Iterator<Map.Entry<Block,Buffer>> i= bufferPoolMap.entrySet().iterator();
			int min_lsn=(int)(Double.POSITIVE_INFINITY);
			Buffer minBuffer=null;

			while(i.hasNext())
			{
				Map.Entry<Block,Buffer> temp= i.next();
				if(!temp.getValue().isPinned() && temp.getValue().getLSN()>-1)
				{
					if(temp.getValue().getLSN()<min_lsn)
					{
						min_lsn=temp.getValue().getLSN();
						minBuffer=temp.getValue();
					}
				}
			}

			if(minBuffer!=null)
				return minBuffer;
			else
			{
				for (Buffer buff : bufferpool)
					if (!buff.isPinned())
						return buff;
			} 
		}
		return null;
	}
	
	//TASK 3: Printing read and write statistics for Buffers.
	public void getStatistics()
	{
		int i=0;
		for (Buffer buff : bufferpool)
		{
			System.out.println("Details for Buffer: "+i);
			System.out.println("Read Count :"+buff.getCountRead());
			System.out.println("Write Count :"+buff.getCountWrite());
			++i;
		}
	}

	boolean containsMapping(Block blk)
	{
		return bufferPoolMap.containsKey(blk);
	}

	Buffer getMapping(Block blk)
	{
		return bufferPoolMap.get(blk);
	}

}
