package org.scache.network.ub;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class SCacheUrmaTest {
    static int tcpBytes=0; static long urmaBytes=0; static int up=0,fe=0,err=0;
    public static void main(String[] a) throws Exception {
        if(a.length<1){System.err.println("server|client");return;}
        boolean srv=a[0].equals("server");
        int port=19090; String dev="openurma0", server="127.0.0.1:19090"; int bs=4096,iters=5;
        for(int i=0;i<a.length;i++){switch(a[i]){case"--port":case"--control-port":port=Integer.parseInt(a[++i]);break;case"--device":dev=a[++i];break;case"--server":server=a[++i];break;case"--block-size":bs=Integer.parseInt(a[++i]);break;case"--iterations":iters=Integer.parseInt(a[++i]);break;}}
        if(srv)server(port,dev);else client(server,dev,bs,iters);
    }
    static void server(int port,String dev) throws Exception {
        ByteBuffer buf=ByteBuffer.allocateDirect(64*1024*1024);
        UrmaTransport t=UrmaTransport.open(dev,128,4096,false);
        t.registerBuffer(buf); EndpointDescriptor lep=t.getLocalEndpoint();
        log("[Srv] ready: "+lep);
        try(ServerSocket ss=new ServerSocket(port,5,InetAddress.getLoopbackAddress())){
            log("[Srv] listen "+port);
            Socket sock=ss.accept(); log("[Srv] client "+sock.getRemoteSocketAddress());
            DataInputStream in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            DataOutputStream out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            byte[] lb=lep.toBytes(); out.writeInt(lb.length); out.write(lb); out.flush(); tcpBytes+=4+lb.length;
            int el=in.readInt(); tcpBytes+=4; byte[] eb=new byte[el]; in.readFully(eb); tcpBytes+=el;
            EndpointDescriptor rep=EndpointDescriptor.fromBytes(eb);
            t.connect(rep); log("[Srv] connected: "+rep);
            RemoteBuffer remote=new RemoteBuffer(rep.segmentAddress,rep.segmentLength,rep.segmentToken);
            while(true){
                int cmd=in.readInt(); tcpBytes+=4; if(cmd==0xFF)break;
                if(cmd==0x03){
                    long crc=in.readLong(); int blen=in.readInt(); tcpBytes+=12;
                    byte[] data=new byte[blen]; buf.position(0); buf.get(data,0,blen);
                    CRC32 c=new CRC32(); c.update(data);
                    boolean ok=(c.getValue()==crc);
                    out.writeByte(ok?1:0); out.flush(); tcpBytes+=1;
                    log("[Srv] Upload+Verify "+blen+"B: "+(ok?"OK":"CRC_FAIL"));
                }
            }
            sock.close();
        }finally{t.close();}
    }
    static void client(String server,String dev,int bs,int iters) throws Exception {
        String[] hp=server.split(":"); String host=hp[0]; int port=Integer.parseInt(hp[1]);
        ByteBuffer buf=ByteBuffer.allocateDirect(Math.max(bs*2,64*1024*1024));
        UrmaTransport t=UrmaTransport.open(dev,128,4096,false);
        t.registerBuffer(buf); EndpointDescriptor lep=t.getLocalEndpoint();
        try(Socket sock=new Socket(InetAddress.getLoopbackAddress(),port)){
            DataInputStream in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            DataOutputStream out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            int el=in.readInt(); tcpBytes+=4; byte[] eb=new byte[el]; in.readFully(eb); tcpBytes+=el;
            EndpointDescriptor rep=EndpointDescriptor.fromBytes(eb);
            byte[] lb=lep.toBytes(); out.writeInt(lb.length); out.write(lb); out.flush(); tcpBytes+=4+lb.length;
            t.connect(rep); RemoteBuffer remote=new RemoteBuffer(rep.segmentAddress,rep.segmentLength,rep.segmentToken);
            log("[Cli] connected: "+rep);
            for(int iter=0;iter<iters;iter++){
                log("[Cli] Iter "+(iter+1));
                byte[] ptn=new byte[bs]; for(int i=0;i<bs;i++)ptn[i]=(byte)((i*131+iter)&0xFF);
                CRC32 crc=new CRC32(); crc.update(ptn); long expCrc=crc.getValue();
                // UPLOAD via URMA (chunked for >4096)
                buf.clear(); buf.put(ptn); buf.flip();
                long wr;
                if (bs <= 4096) wr = t.write(remote, buf, 0, bs);
                else wr = t.writeChunked(remote, buf, 0, bs);
                urmaBytes += bs;
                int ws = t.waitFor(wr, (bs > 4096 ? 300000 : 60000));
                if (ws != 0) { err++; continue; }
                // TCP: tell server
                out.writeInt(0x03); out.writeLong(expCrc); out.writeInt(bs); out.flush(); tcpBytes+=16;
                boolean ok=in.readByte()==1; tcpBytes+=1;
                if(!ok){err++; continue;}
                up++;
                // FETCH via URMA (chunked for >4096)
                buf.clear();
                long rr;
                if (bs <= 4096) rr = t.read(remote, buf, 0, bs);
                else rr = t.readChunked(remote, buf, 0, bs);
                urmaBytes += bs;
                int rs = t.waitFor(rr, (bs > 4096 ? 300000 : 60000));
                if (rs != 0) { err++; continue; }
                byte[] got=new byte[bs]; buf.position(0); buf.get(got);
                CRC32 fcrc=new CRC32(); fcrc.update(got);
                if(fcrc.getValue()!=expCrc){err++; continue;}
                fe++;
            }
            out.writeInt(0xFF); out.flush(); tcpBytes+=4;
        }finally{t.close();print(bs,iters);}
    }
    static void log(String m){System.out.println(m);}
    static void print(int bs,int iters){
        boolean pass=up==iters&&fe==iters&&err==0;
        System.out.println("\n=== SCache URMA RESULTS ===");
        System.out.println("block_size="+bs+"\niterations="+iters);
        System.out.println("uploaded="+up+"\nfetched="+fe+"\nerrors="+err);
        System.out.println("tcp_bytes="+tcpBytes+"\nurma_bytes="+urmaBytes);
        System.out.println("result="+(pass?"PASS":"FAIL"));
    }
}
