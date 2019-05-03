--{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Data.Char
import Data.Maybe
import Data.List
import qualified Database.Zookeeper as Z
import qualified Data.ByteString.Char8 as B
import System.Console.GetOpt
import System.Environment
import System.Exit
import System.IO
import Text.Read

data Operation = Dump | Get | Set | Create | Delete | Stat | Children | GetAcl
   deriving (Show, Read, Eq)

data Scheme = Digest | IP | Host
   deriving (Show, Read, Eq)

data Options = Options { 
     optZookeeper :: String,
     optOperation :: Operation,
     optPath :: Maybe String,
     optValue :: Maybe String,
     optScheme :: Maybe Scheme,
     optCredentials :: Maybe String
   } deriving Show

ops = [(Dump,     opDump), 
       (Get,      opGet),
       (Set,      opSet),
       (Create,   opCreate),
       (Delete,   opDelete),
       (Stat,     opStat),
       (Children, opChildren),
       (GetAcl,   opGetAcl)
      ]

defaultOptions :: Options
defaultOptions = Options { 
      optZookeeper = "",
      optOperation = Dump,
      optPath = Nothing,
      optValue = Nothing,
      optScheme = Nothing,
      optCredentials = Nothing
   }

options :: [OptDescr (Options -> IO Options)]
options =
   [ Option ['h'] ["help"]
      (NoArg (\_ -> exitUsage))
      "Show help"
   , Option ['z'] ["zookeeper"]
      (ReqArg (\arg opt -> return opt { optZookeeper = arg }) "CONN")
      "Zookeeper connection string"
   , Option ['o'] ["operation"]
      (ReqArg (\arg opt -> do
          let eitherOp = readEither arg :: Either String Operation
          case eitherOp of
             Left _   -> hPutStrLn stderr ("Unknown operation: " ++ arg) >> exitWith (ExitFailure 1)
             Right op -> return opt { optOperation = op }
         ) "OPERATION")
      "Operation: Dump, Get, Set, Create, Delete, Stat, Children, GetAcl"
   , Option ['p'] ["path"]
      (ReqArg (\arg opt -> return opt { optPath = Just arg }) "PATH")
      "Path"
   , Option ['s'] ["scheme"]
      (ReqArg (\arg opt -> do
          let eitherScheme = readEither arg :: Either String Scheme
          case eitherScheme of
             Left _       -> hPutStrLn stderr ("Unknown scheme: " ++ arg) >> exitWith (ExitFailure 1)
             Right scheme -> return opt { optScheme = Just scheme }
         ) "SCHEME")
      "Authentication scheme: Digest"
   , Option ['c'] ["credentials"]
      (ReqArg (\arg opt -> return opt { optCredentials = Just arg }) "CREDENTIALS")
      "Credentials depending on scheme"
   , Option ['v'] ["value"]
      (ReqArg (\arg opt -> return opt { optValue = Just arg }) "VALUE")
      "Value"
   ]

exitUsage = do
   hPutStrLn stderr usage 
   exitWith (ExitFailure 1)

usage = usageInfo header options
   where
      header = "Usage: zk-tool [OPTION...]"

main = do
   args <- getArgs
   let (actions, nonOptions, errors) = getOpt RequireOrder options args
   opts <- foldl (>>=) (return defaultOptions) actions

   when (optZookeeper opts == "") exitUsage

   Z.setDebugLevel Z.ZLogError

   Z.withZookeeper (optZookeeper opts) 1000 Nothing Nothing $ \zh -> do 
      maybe (return ()) (\_ -> addAuth zh opts) (optScheme opts)
      let op = snd $ fromJust $ find ((==) (optOperation opts) . fst) ops in
         op zh opts

addAuth :: Z.Zookeeper -> Options -> IO ()
addAuth zh opts = credentials opts >>= addAuthScheme zh (scheme opts)
   where
      scheme = map toLower . show . fromJust . optScheme
      credentials opts = maybe (ioError $ userError "Credentials are mandatory if scheme is set") return (optCredentials opts)

addAuthScheme :: Z.Zookeeper -> String -> String -> IO ()
addAuthScheme zh scheme credentials = Z.addAuth zh scheme (B.pack credentials) $ \result -> do
   case result of
      Left e  -> hPutStrLn stderr ("Authentication error: " ++ (show e)) >> exitWith (ExitFailure 1)
      Right _ -> return ()

withPath :: Options -> (String -> IO ()) -> IO ()
withPath opts fun = maybe error fun $ optPath opts
   where
      error = hPutStrLn stderr "Path option is mandatory" >> exitWith (ExitFailure 1)

getValue :: Options -> Maybe B.ByteString
getValue opts = optValue opts >>= return . B.pack

checkResult :: Either Z.ZKError a -> IO a
checkResult result = either (ioError . userError . show) return result

opCreate :: Z.Zookeeper -> Options -> IO ()
opCreate zh opts = withPath opts $ \path -> Z.create zh path (getValue opts) Z.OpenAclUnsafe [] >>= checkResult >>= putStrLn

opSet :: Z.Zookeeper -> Options -> IO ()
opSet zh opts = withPath opts $ \path -> Z.set zh path (getValue opts) Nothing >>= checkResult >>= putStrLn . show

opGet :: Z.Zookeeper -> Options -> IO ()
opGet zh opts = withPath opts $ \path -> Z.get zh path Nothing >>= liftM fst . checkResult >>= return . fromMaybe "" . liftM B.unpack >>= putStrLn

opStat :: Z.Zookeeper -> Options -> IO ()
opStat zh opts = withPath opts $ \path -> Z.get zh path Nothing >>= liftM snd . checkResult >>= printStat
   where printStat stat = putStrLn $ "CzxId: " ++ (show $ Z.statCzxId stat)
                               ++ "\nPzxId: " ++ (show $ Z.statPzxId stat)
                               ++ "\nCreatetime: " ++ (show $ Z.statCreatetime stat)
                               ++ "\nModifytime: " ++ (show $ Z.statModifytime stat)
                               ++ "\nVersion: " ++ (show $ Z.statVersion stat)
                               ++ "\nChildrenVersion: " ++ (show $ Z.statChildrenVersion stat)
                               ++ "\nAclVersion: " ++ (show $ Z.statAclVersion stat)
                               ++ "\nDataLength: " ++ (show $ Z.statDataLength stat)
                               ++ "\nNumChilderen: " ++ (show $ Z.statNumChildren stat)
                               ++ "\nEphemeralOwner: " ++ (maybe "n/a" show (Z.statEphemeralOwner stat))

opDelete :: Z.Zookeeper -> Options -> IO ()
opDelete zh opts = withPath opts $ \path -> Z.delete zh path Nothing >>= checkResult

opGetAcl :: Z.Zookeeper -> Options -> IO ()
opGetAcl zh opts = withPath opts $ \path -> Z.getAcl zh path >>= liftM fst . checkResult >>= printAcls

printAcls :: Z.AclList -> IO ()
printAcls acls = case acls of
                    Z.List acl -> forM_ acl printAcl
                    acl        -> putStrLn $ show acl
   where
      printAcl acl = putStrLn $ "Scheme: " ++ Z.aclScheme acl 
                           ++ ", Id: " ++ Z.aclId acl 
                           ++ ", Flags: " ++ getFlags acl
      getFlags = intercalate ", " . liftM show . Z.aclFlags

opChildren :: Z.Zookeeper -> Options -> IO ()
opChildren zh opts = withPath opts $ \path -> Z.getChildren zh path Nothing >>= checkResult >>= mapM_ putStrLn

opDump :: Z.Zookeeper -> Options -> IO ()
opDump zh opts = do
      printNodes zh 0 $ maybe "/" id $ optPath opts
   where
      printNodes zh level node = do
         maybeValue <- Z.get zh node Nothing
         let value = either (\_ -> Nothing) fst maybeValue in
            printNode level node $ fromMaybe "" (liftM B.unpack value)

         maybeNodes <- Z.getChildren zh node Nothing

         let nodes = either (\_ -> []) id maybeNodes in
            forM_ nodes (\n -> printNodes zh (level+1) $ joinNodes node n)

      joinNodes n1 n2 = case (n1, n2) of
                           ("/", n2) -> "/" ++ n2
                           (n1,  n2) -> n1 ++ "/" ++ n2

printNode :: Int -> String -> String -> IO ()
printNode level node "" = putStrLn $ line level node
printNode level node value = putStrLn $ line level node ++ "\n  " ++ line level value

line :: Int -> String -> String
line n val = (concat $ take (n*2) $ repeat " ") ++ val
