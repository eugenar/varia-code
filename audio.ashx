<%@ WebHandler Language="C#" Class="media.Audio" %>

using System;
using System.Web;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;

using BeetUtil;

namespace media
{
    public static class RequestThreadPool
    {
        static readonly int POOL_SIZE = int.Parse(System.Configuration.ConfigurationManager.AppSettings["threadpool_size"]);
        internal static readonly int MAX_SLEEP = int.Parse(System.Configuration.ConfigurationManager.AppSettings["thread_max_sleep"]); // ms
        private static List<AsyncResult> requests;
        private static List<Thread> threads;

        static RequestThreadPool()
        {
            threads = new List<Thread>(POOL_SIZE);
            requests = new List<AsyncResult>();

            for (int i = 0; i < POOL_SIZE; i++)
            {
                Thread t = CreateWorker();
                threads.Add(t);
                t.Start();
            }
        }

        static Thread CreateWorker()
        {
            Thread t = new Thread(Run);
            t.IsBackground = true;

            return t;
        }
        
        public static void KillWorkers() {
			foreach( Thread t in RequestThreadPool.threads ) {
				t.Abort();
			}
        }

        internal static void AddRequest(AsyncResult result)
        {
            if (result == null) throw new ArgumentNullException("result");

            // monitor the worker threads and replace the ones that aren't alive
            for (int i = 0; i < threads.Count; i++)
            {
                if (threads[i] == null || !threads[i].IsAlive)
                {
                    threads[i] = CreateWorker();
                    threads[i].Start();

                    InvokeUtil.LogException(Guid.Empty, new Exception(String.Format("Replacing dead media worker in slot {0}.", i))
                    );
                }
            }            
            
            result.executionContext = ExecutionContext.Capture();
            lock (requests)
            {
                requests.Add(result);
                Monitor.PulseAll(requests);
            }
        }

        private static void Run()
        {
            try
            {
                while (true)
                {
                    int sleep = MAX_SLEEP;
                    Monitor.Enter(requests);
                    if (requests.Count == 0)
                        Monitor.Wait(requests);

                    for (int i = 0; i < requests.Count; i++)
                    {
                        if (requests[i].state.completed)
                            requests.RemoveAt(i);
                        else if (requests[i].state.ReadyToRun())
                        {
                            requests[i].state.serviced = true; // ensure no other thread will touch this request
                            Monitor.Exit(requests); // free the lock so that processing doesn't block other threads
                            ExecutionContext.Run(ExecutionContext.Capture(), new ContextCallback(requests[i].waitCallback), requests[i]); // here we do the time consuming I/O
                            Monitor.Enter(requests); // re-acquire the lock and continue loop
                        }
                        else if (!requests[i].state.serviced && requests[i].state.sleep < sleep)
                            sleep = requests[i].state.sleep > 0 ? requests[i].state.sleep : 0; // sleep for the minimum amount from all the sleeping requests
                    }
                    Monitor.Exit(requests);
                    Thread.Sleep(sleep);
                }
            }
            catch (Exception e)
            {
                // getting here will let the worker thread to terminate. dead workers must be replaced, see AddRequest
                // we don't have http session state around at this point...
                InvokeUtil.LogException( Guid.Empty, e);
            }            
        }
        
    }

    /// <summary>
    /// Encapsulates the state of a request
    /// </summary>
    internal class State
    {
        public State(DateTime lastService, int sleep, bool serviced, bool completed)
        {
            this.lastService = lastService;
            this.sleep = sleep;
            this.serviced = serviced;
            this.completed = completed;
        }

        public bool ReadyToRun()
        {
            sleep -= (int)((TimeSpan)DateTime.Now.Subtract(lastService)).TotalMilliseconds;
            lastService = DateTime.Now;
            return !serviced && sleep <= 0;
        }

        public DateTime lastService;
        public int sleep;
        public bool serviced;
        public bool completed;
    }

    /// <summary>
    /// Encapsulates an asynchronous request
    /// </summary>
    internal class AsyncResult : IAsyncResult
    {
        internal readonly Stream stream;
        internal byte[] buffer;
        internal State state;
        internal ExecutionContext executionContext;
        internal readonly HttpContext httpContext;
        internal readonly AsyncCallback callback;
        internal readonly WaitCallback waitCallback;
        internal MediaObj mediaObj;
        internal int sent;
        internal DateTime startTime;
        internal bool started = false;
        private bool isComplete = false;
        private bool isSync = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="httpContext">Current Http Context</param>
        /// <param name="callback">Callback used by ASP.NET</param>
        /// <param name="waitCallback">Callback used by RequestThreadPool</param>
        /// <param name="mo">media object set by the calling thread</param>
        public AsyncResult(HttpContext httpContext, AsyncCallback callback, WaitCallback waitCallback, MediaObj mo)
        {
            this.httpContext = httpContext;
            if ( !httpContext.Request.UserAgent.Contains( "MSIE" ) ) {
				httpContext.Response.Cache.SetCacheability(HttpCacheability.NoCache);
			} else {
				httpContext.Response.Cache.SetCacheability(HttpCacheability.Private);
				httpContext.Response.Cache.SetExpires(DateTime.Now.AddYears(-1));
			}
			httpContext.Response.Cache.SetRevalidation(HttpCacheRevalidation.AllCaches);
            httpContext.Response.BufferOutput = false;
            mediaObj = mo;
            buffer = new byte[Audio.BUFFER_SIZE];
            stream = mediaObj.Stream();
            this.callback = callback;
            this.waitCallback = waitCallback;
            this.state = new State(DateTime.Now, mo.mode == "download" ? RequestThreadPool.MAX_SLEEP : 0, false, false); // sleep constant for downloads
            startTime = DateTime.Now;
        }


        // used to immediately end the async request without doing work
        public AsyncResult(HttpContext httpContext, AsyncCallback callback)
        {
            this.httpContext = httpContext;
            if ( httpContext.Request.UserAgent.Contains( "MSIE" ) ) {
				httpContext.Response.Cache.SetCacheability(HttpCacheability.NoCache);
			} else {
				httpContext.Response.Cache.SetCacheability(HttpCacheability.Private);
				httpContext.Response.Cache.SetExpires(DateTime.Now.AddYears(-1));
			}
			httpContext.Response.Cache.SetRevalidation(HttpCacheRevalidation.AllCaches);
			httpContext.Response.BufferOutput = false;
            this.stream = null;
            this.isComplete = true;
            this.isSync = true;
            this.callback = callback;
        }
        
        // stream STREAM_START s
        internal protected void StreamStart() 
        {
            try
            {
                int count = 0;
                int sent = 0;
                if (mediaObj.mode == "sample_link")
                    stream.Seek((long)(float.Parse(mediaObj.data)*stream.Length), SeekOrigin.Begin); // position to read from offset
                // read from file into buffer and write buffer to the response stream
                while (sent < Audio.STREAM_START * mediaObj.bitRate * 1000 / 8) // Loop until STREAM_START sec sent
                {
                    if (httpContext.Response.IsClientConnected && (count = stream.Read(buffer, 0, Audio.BUFFER_SIZE)) > 0) // client connected and not eof
                    {
                        httpContext.Response.OutputStream.Write(buffer, 0, count);
                        sent += count;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(httpContext), e);
            }
        }


        /// <summary>
        /// Completes the request
        /// </summary>
        public void Complete()
        {
            // close stream
            if (stream != null)
            {
                try
                {
                    stream.Close();
                }
                catch (Exception e)
                {
                    InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(httpContext), e);
                }
            }
            //state.completed = true; // set request completed; will be removed from the threadpool requests
            isComplete = true;

            // Call any registered callback (ASP.NET pipeline)
            if (callback != null)
            {
                callback(this); // calls EndRequest; IIS ends the request
            }
        }


        // IAsyncResult methods (must implement)
        /// <summary>
        /// Gets the object on which one could perform a lock
        /// </summary>
        public object AsyncState
        {
            get { return this.state; }
        }

        /// <summary>
        /// Should return true if something all work was done in begin request
        /// </summary>
        public bool CompletedSynchronously
        {
            get { return this.isSync; }
        }

        /// <summary>
        /// Gets a handle used to synchronize access to shared resources
        /// </summary>
        public WaitHandle AsyncWaitHandle
        {
            get
            {
                return null; // not used
            }
        }

        /// <summary>
        /// Gets the current status of the request
        /// </summary>
        public bool IsCompleted
        {
            get { return this.isComplete; }
        }

    }
    
    
    public class Audio : IHttpAsyncHandler
    {
        public static readonly float LIB_STREAM_RATIO = float.Parse(System.Configuration.ConfigurationManager.AppSettings["lib_stream_ratio"]);
        public static readonly float SAMPLE_STREAM_RATIO = float.Parse(System.Configuration.ConfigurationManager.AppSettings["sample_stream_ratio"]);
        public static readonly float RADIO_STREAM_RATIO = float.Parse(System.Configuration.ConfigurationManager.AppSettings["radio_stream_ratio"]);
        public static readonly int BUFFER_SIZE = int.Parse(System.Configuration.ConfigurationManager.AppSettings["buffer_size"]);
        public static readonly int STREAM_START = int.Parse(System.Configuration.ConfigurationManager.AppSettings["stream_start"]); // duration in sec to stream at the beginning of the request

        public Audio()
        {
        }

        public bool IsReusable { get { return true; } }

        public void ProcessRequest(HttpContext context)
        {
            // never called in an asynchronous handler
            throw new InvalidOperationException();
        }


        public IAsyncResult BeginProcessRequest(HttpContext context, AsyncCallback cb, Object state)
        {
            try
            {
                // secret hook for killing all streaming media workers
                if (context.Request.QueryString["kill"] == "necodo99")
                {
                    RequestThreadPool.KillWorkers();
                    context.Response.Write("all media workers were killed");
                    return new AsyncResult(context, cb);
                }
                string s = context.Request.QueryString["s"]; // check for audio streaming
                if (s != null)
                {
                    HttpCookie c = InvokeUtil.GetCookie(context.Request.Cookies, s);
                    if (null == c)
                    {
                        InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(context), new Exception("cookie is null"));
                        // end request without work
                        return new AsyncResult(context, cb);
                    }
                    string urlEncrypt = c.Value;
                    if (null == urlEncrypt)
                    {
                        InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(context), new Exception("urlEncrypt is null"));
                        // end request without work
                        return new AsyncResult(context, cb);
                    }
                    else
                    {
                        MediaObj mo = MediaObj.Decrypt(urlEncrypt);
                        if (System.DateTime.UtcNow.Ticks < mo.expire)
                        {
                            AsyncResult result = null;
                            context.Response.AppendHeader("Content-Length", mo.Stream().Length.ToString());
                            switch (mo.mode)
                            {
                                case "sample": // fall through
                                case "sample_link":
                                case "release_radio":
                                case "people_radio":
                                case "person_radio":
                                case "artist_radio":
                                case "feature_radio":
                                case "decibel_radio":
                                case "lib":
                                    context.Response.AppendHeader("Content-Type", "audio/mpeg");
                                    result = new AsyncResult(context, cb, new WaitCallback(ProcessAudio), mo);
                                    break;
                                case "waveform":
                                    context.Response.AppendHeader("Content-Type", "application/xml");
                                    result = new AsyncResult(context, cb, new WaitCallback(ProcessWaveform), mo);
                                    break;
                                default:
                                    InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(context), new Exception("Invalid querystring in url"));
                                    return new AsyncResult(context, cb);
                            }
                            RequestThreadPool.AddRequest(result);
                            return result;
                        }
                        else // url valability expired
                        {
                            // end request without work
                            return new AsyncResult(context, cb);
                        }
                    }
                }
                s = context.Request.QueryString["d"]; // check for download
                if (s != null)
                {
                    MediaObj mo = MediaObj.Decrypt(s.Replace(" ", "+")); // querystring is parsed wrong! "+" is replaced with " "; must put it back
                    if (System.DateTime.UtcNow.Ticks < mo.expire && mo.ValidateMediaToken())
                    {
                        context.Response.AppendHeader("Content-Length", mo.Stream().Length.ToString());
                        context.Response.AppendHeader("Content-Disposition", "attachment;filename=\"" + mo.save_to_file + "\"");
                        if (mo.media_tag == "wav")
                            context.Response.AppendHeader("Content-Type", "audio/x-wav");
                        else
                            context.Response.AppendHeader("Content-Type", "audio/mpeg");
                        mo.event_tag = mo.mode + "_start";
                        mo.Log(); // log event start
                        AsyncResult result = new AsyncResult(context, cb, new WaitCallback(ProcessDownload), mo);
                        result.state.sleep = RequestThreadPool.MAX_SLEEP; // constant sleep for downloads
                        RequestThreadPool.AddRequest(result);
                        return result;
                    }
                    else // url valability expired
                    {
                        SendGenericTxt(context);
                        // end request without work
                        return new AsyncResult(context, cb);
                    }
                }
                // check for viral download
                s = context.Request.QueryString["trid"];
                string email = context.Request.QueryString["email"];
                if (!string.IsNullOrEmpty(s) && !string.IsNullOrEmpty(email))
                {
                    string cookie_name = "vradio";
                    HttpCookie cookie = context.Request.Cookies.Get(cookie_name);
                    string user_cookie = (cookie == null) ? Guid.NewGuid().ToString() : cookie.Value;
                    if (cookie == null || string.IsNullOrEmpty(cookie.Value))
                    {
                        // set cookie
                        cookie = new HttpCookie(cookie_name, user_cookie);
                        cookie.Expires = DateTime.Now.AddYears(999);
                        context.Response.Cookies.Add(cookie);
                    }
                    string ip_address = context.Request.ServerVariables["HTTP_X_FORWARDED_FOR"];
                    if (string.IsNullOrEmpty(ip_address))
                        ip_address = context.Request.ServerVariables["REMOTE_ADDR"];
                    Int64 ip_number = string.IsNullOrEmpty(ip_address) ? 0 : InvokeUtil.IPAddressToIPNumber(ip_address);
                    MediaObj mo = new MediaObj(s, email, user_cookie, ip_number);
                    if (mo != null)
                    {
                        context.Response.AppendHeader("Content-Length", mo.Stream().Length.ToString());
                        context.Response.AppendHeader("Content-Disposition", "attachment;filename=\"" + mo.save_to_file + "\"");
                        if (mo.media_tag == "wav")
                            context.Response.AppendHeader("Content-Type", "audio/x-wav");
                        else
                            context.Response.AppendHeader("Content-Type", "audio/mpeg");
                        AsyncResult result = new AsyncResult(context, cb, new WaitCallback(ProcessDownload), mo);
                        RequestThreadPool.AddRequest(result);
                        return result;
                    }
                }
                // check for sample
                s = context.Request.QueryString["sa"];
                if (!string.IsNullOrEmpty(s) )
                {
                    MediaObj mo = MediaObj.Decrypt(s.Replace(" ", "+"));
                    context.Response.AppendHeader("Content-Type", "audio/mpeg");
                    int streamLength = Audio.STREAM_START*mo.bitRate*1000/8; // 30s worth of bytes
                    context.Response.AppendHeader("Content-Length", streamLength.ToString());
                    AsyncResult result = new AsyncResult(context, cb, new WaitCallback(ProcessAudio), mo);
                    RequestThreadPool.AddRequest(result);
                    return result;
                }
            }
            catch (Exception e)
            {
                InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(context), e);
                // end request without work
                return new AsyncResult(context, cb);
            }
            return new AsyncResult(context, cb);
        }


        private void ProcessAudio(object ar)
        {
            AsyncResult asyncResult = ar as AsyncResult;
            if (asyncResult != null)
            {
                if (!asyncResult.started)
                {
                    asyncResult.StreamStart(); // stream 30 s only once at the beginning
                    if (asyncResult.mediaObj.mode == "sample_link")
                    {
                        asyncResult.mediaObj.event_tag = asyncResult.mediaObj.mode;
                        asyncResult.mediaObj.Log();
                        asyncResult.Complete(); // sample-links are 30 s
                        return;
                    }
                    else
                    {
                        asyncResult.mediaObj.event_tag = asyncResult.mediaObj.mode + "_start";
                        asyncResult.mediaObj.Log(); // log event start
                        asyncResult.started = true;
                    }
                }
                try
                {
                    float streamRatio = asyncResult.mediaObj.mode.EndsWith("radio") ? RADIO_STREAM_RATIO : (asyncResult.mediaObj.mode == "sample" ? SAMPLE_STREAM_RATIO : LIB_STREAM_RATIO);
                    int count = 0;
                    while (asyncResult.httpContext.Response.IsClientConnected && (count = asyncResult.stream.Read(asyncResult.buffer, 0, BUFFER_SIZE)) > 0) // Loop until end of file stream or client disconnected
                    {
                        asyncResult.httpContext.Response.OutputStream.Write(asyncResult.buffer, 0, count);
                        asyncResult.sent += count;
                        double ahead = asyncResult.sent * 8 - ((TimeSpan)DateTime.Now.Subtract(asyncResult.startTime)).TotalMilliseconds * asyncResult.mediaObj.bitRate * streamRatio;
                        if (ahead > 0)
                        {
                            asyncResult.state.lastService = DateTime.Now;
                            asyncResult.state.sleep = (int)(ahead / (asyncResult.mediaObj.bitRate * streamRatio));
                            asyncResult.state.serviced = false;
                            return;
                        }
                    }
                    // everything was streamed or client disconnected; log
                    if (asyncResult.mediaObj.mode.EndsWith("radio") && ((TimeSpan)DateTime.Now.Subtract(asyncResult.startTime)).TotalSeconds < 30) // don't have to pay for this
                        asyncResult.mediaObj.event_tag = asyncResult.mediaObj.mode + "_incomplete";
                    else
                        asyncResult.mediaObj.event_tag = asyncResult.mediaObj.mode + "_complete";
                    asyncResult.mediaObj.Log();
                    asyncResult.Complete();
                    asyncResult.state.completed = true; // first let the request complete, then mark task as completed; will be removed
                }
                catch (Exception e)
                {
                    InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(asyncResult.httpContext), e);
                    asyncResult.Complete();
                }
            }
        }


        private void ProcessWaveform(object ar)
        {
            AsyncResult asyncResult = ar as AsyncResult;
            if (asyncResult != null)
            {
                try
                {
                    int count = 0;
                    // read from file into buffer and write buffer to the response stream
                    while (asyncResult.httpContext.Response.IsClientConnected && (count = asyncResult.stream.Read(asyncResult.buffer, 0, BUFFER_SIZE)) > 0) // Loop until end of file stream or client disconnected
                    {
                        asyncResult.httpContext.Response.OutputStream.Write(asyncResult.buffer, 0, count);
                    }
                }
                catch (Exception e)
                {
                    InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(asyncResult.httpContext), e);
                }
                finally
                {
                    // request complete
                    asyncResult.Complete();
                    asyncResult.state.completed = true;
                }
            }
        }


        private void ProcessDownload(object ar)
        {
            AsyncResult asyncResult = ar as AsyncResult;
            if (asyncResult != null)
            {
                try
                {
                    if (asyncResult.httpContext.Response.IsClientConnected)
                    {
                        int count = 0;
                        for (int i = 0; i < 10; i++) // read/write in 5Mbs chuncks; standard cable/dsl download speed
                        {
                            count = asyncResult.stream.Read(asyncResult.buffer, 0, BUFFER_SIZE); // read from file into buffer

                            if (count > 0) // write buffer to the response stream
                            {
                                asyncResult.httpContext.Response.OutputStream.Write(asyncResult.buffer, 0, count);
                            }
                            else // log
                            {
                                if (asyncResult.mediaObj.mode == "viral_download")
                                    asyncResult.mediaObj.LogViralDownload();
                                else
                                {
                                    asyncResult.mediaObj.event_tag = asyncResult.mediaObj.mode + "_complete";
                                    asyncResult.mediaObj.Log();
                                    asyncResult.mediaObj.UpdateMediaToken();
                                }
                                break;
                            }
                        }
                        if (count == 0)
                        {
                            asyncResult.Complete();  // request complete
                            asyncResult.state.completed = true;
                        }
                        else
                        {
                            asyncResult.state.lastService = DateTime.Now;
                            asyncResult.state.serviced = false;
                        }
                    }
                    else
                    {
                        asyncResult.Complete();  // request complete
                        asyncResult.state.completed = true;
                    }
                }
                catch (Exception e)
                {
                    InvokeUtil.LogException(InvokeUtil.GetSeesionIdFromContext(asyncResult.httpContext), e);
                    asyncResult.Complete(); // request complete
                }
            }
        }


        private void SendGenericTxt(HttpContext context)
        {
            context.Response.AppendHeader("Content-Type", "text/html");
            context.Response.AppendHeader("Content-Disposition", "attachment;filename=\"readme.txt\"");
            context.Response.Flush(); // force browser to open Save To... dialog

            try
            {
                context.Response.TransmitFile(context.Request.ApplicationPath + "/media/readme.txt");
            }
            finally
            {
                //??
            };
        }
        

        public void EndProcessRequest(IAsyncResult ar)
        {
            // IIS ends request
        }
    }

}

