package simpledb.server;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.buffer.*;

public class TestCode {

	public TestCode() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		// configure and initialize the database
		SimpleDB.init("simpledb");

		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("simpledb", d);
		
		//STEP 1: Create a list of files-blocks.
		Block block[]= new Block[10]; 
		
		//STEP 2: Create a list of buffers.
		Buffer buffer[]=new Buffer[8];
		
		for(int i=0;i<block.length;i++){
			block[i]= new Block("fldcat.tbl",i); 
		}

		for(int j=0;j<buffer.length;j++){

			buffer[j]= new Buffer();
		}
		
		BufferMgr buffermgr= new BufferMgr(buffer.length);
		
		//STEP 3: Check the number of available buffers initially.
		System.out.println("STEP 3: ");
		System.out.println("Initially, number of available buffers:"+buffermgr.available());
		System.out.println("Number of buffers created is "+buffer.length);
		System.out.println("***************************************************************************");

		
		//STEP 4: Keep pinning buffers one by one and check the number of available buffers.
		//STEP 10: Check if the map for the (block, buffer) whenever you pin a new buffer to a block and replace a buffer and make sure it reflects the latest mappings.
		HashMap<Block, Buffer> hashmap_status=new HashMap<Block, Buffer>();
		
		buffermgr.pin(block[0]);
		hashmap_status=buffermgr.getBufferMgr().getBufferPoolMap();
		
		System.out.println("The Map status is "+hashmap_status);
		System.out.println("Number of available buffers: "+buffermgr.available());
		System.out.println();

		buffermgr.pin(block[1]);
		hashmap_status=buffermgr.getBufferMgr().getBufferPoolMap();
		System.out.println("The Map status is "+hashmap_status);
		System.out.println("Number of available buffers: "+buffermgr.available());
		System.out.println();

		buffermgr.pin(block[2]);
		hashmap_status=buffermgr.getBufferMgr().getBufferPoolMap();
		System.out.println("The Map status is "+hashmap_status);
		System.out.println("Number of available buffers: "+buffermgr.available());
		System.out.println();
		
		//STEP 5:When all buffers have been pinned, if pin request is made again, throw an exception. 
		//UNCOMMENT BELOW BLOCK TO TEST STEP 5
		/*for(int i=3;i<buffer.length;i++)
		{
			buffermgr.pin(block[i]);  //pinning the remaining buffers
			System.out.println("The Map status is "+hashmap_status);
			System.out.println("Number of available buffers: "+buffermgr.available());
			System.out.println();
		}
		buffermgr.pin(block[8]); 
		System.out.println("***************************************************************************");*/
		
		
		//STEP 6:Unpin a few buffers and see if you are still getting an exception or not.
		//UNCOMMENT BELOW BLOCK TO TEST STEP 6
		/*Buffer buff7=buffermgr.getBufferMgr().getBufferPoolMap().get(block[7]);
		buffermgr.unpin(buff7);
		buffermgr.pin(block[8]);
		
		System.out.println("The Map status is "+hashmap_status);
		System.out.println("Number of available buffers: "+buffermgr.available());
		System.out.println();*/
		
		
		//STEP 7:Now to test the replacement policy, call setInt and getInt methods on some pinned buffers.
		
		Buffer buff1=buffermgr.getBufferMgr().getBufferPoolMap().get(block[1]);
		buff1.setInt(0,5,1,10);
		buff1.setString(9,"Test Buffer 1",20,20);

		Buffer buff2=buffermgr.getBufferMgr().getBufferPoolMap().get(block[2]);
		buff2.setInt(10,555,2,30);
		buff2.setString(30,"Test Buffer 2",2,40);

		Buffer buff0=buffermgr.getBufferMgr().getBufferPoolMap().get(block[0]);
		buff0.setInt(80,786,3,50);
		buff0.setString(110,"Test Buffer 0",3,60);

		System.out.println("Retrieved Value from Buffer 1: "+buff1.getInt(0));
		System.out.println("Retrieved Value from Buffer 1: "+buff1.getString(9));
		
		System.out.println("Retrieved Value from Buffer 2: "+buff2.getString(30));
		
		System.out.println("Retrieved Value from Buffer 0: "+buff0.getInt(80));
		System.out.println("***************************************************************************");
		//STEP 12:Whenever you call getInt, setInt methods, keep the running count of the calls and make sure that the counter value is same as the actual number of read/write calls.
		buffermgr.getStatistics();
		
		//STEP 8: Unpin some of the buffers used in the previous step.
		buffermgr.unpin(buff0);
		buffermgr.unpin(buff1);
		System.out.println("Buffer 0 and Buffer 1 have been unpinned." );
		System.out.println("The pincount of Buffer 0 = "+buff0.getPinCount()+" and LSN = "+buff0.getLSN());
		System.out.println("The pincount of Buffer 1 = "+buff1.getPinCount()+" and LSN = "+buff1.getLSN());
		System.out.println("The pincount of Buffer 2 = "+buff2.getPinCount()+" and LSN = "+buff2.getLSN());
		System.out.println("***************************************************************************");
		
		//STEP 9: Try to pin a new buffer and see which current buffer is being replaced. It should be the one that has least recently been modified (ie written to).
		//STEP 10:Check if the map for the (block, buffer) whenever you pin a new buffer to a block and replace a buffer and make sure it reflects the latest mappings.
		System.out.println("The Status of Hashmap before replacement is "+hashmap_status);
		buffermgr.pin(block[5]);
		System.out.println("The Status of Hashmap after replacement is "+hashmap_status);
		System.out.println("***************************************************************************");
	}

}
