{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Data.Aeson hiding (Options, defaultOptions)
import Data.Aeson.Text hiding (Options, defaultOptions)
import Data.Aeson.Types hiding (Options, defaultOptions)
import Data.Char
import Data.Maybe
import Data.List
import Data.List.Split
import qualified Data.Text.Lazy.IO as T
import qualified Data.Text.Lazy.Encoding as T
import qualified Database.Zookeeper as Z
import qualified Data.ByteString.Char8 as B
import System.Console.GetOpt
import System.Environment
import System.Exit
import System.IO
import Text.Read

import Control.Monad.IO.Class

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
   optCredentials :: Maybe String,
   optJson :: Bool
} deriving Show

data Node = Node {
   nodePath :: String,
   nodeName :: String,
   nodeValue :: Maybe String,
   nodeChildren :: Maybe [Node]
} deriving (Show)

instance ToJSON Node where
   toJSON (Node path name value nodes) = do
      object $ ["path" .= path] ++ addValue "name" name ++ addMaybeValue "value" value ++ addMaybeValue "nodes" nodes 

addValue name value = case value of
      "" -> []
      v  -> [name .= v]

addMaybeValue name value = case value of
      Nothing -> []
      Just [] -> []
      Just v  -> [name .= v]

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
      optCredentials = Nothing,
      optJson = False
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
   , Option ['j'] ["json"]
      (NoArg (\opt -> return opt { optJson = True }))
      "Enable JSON output"
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
      result <- buildTree zh $ maybe "/" id $ optPath opts 
      case optJson opts of
         True -> printJson result
         False -> printPlain result
   where
      buildTree zh node = do
         maybeValue <- Z.get zh node Nothing
         maybeNodes <- Z.getChildren zh node Nothing
         children <- getNodes zh node maybeNodes

         return Node {
            nodePath = getPath node,
            nodeName = last $ splitOn "/" node,
            nodeValue = getValue maybeValue,
            nodeChildren = toMaybe children
         }

      getValue maybeValue = let value = either (\_ -> Nothing) fst maybeValue in
            liftM B.unpack value

      getPath node = formatPath $ intercalate "/" $ init $ splitOn "/" node

      formatPath "" = "/"
      formatPath path = path

      getNodes :: Z.Zookeeper -> String -> Either Z.ZKError [String] -> IO [Node]
      getNodes zh node maybeNodes = getNodesAux zh node maybeNodes

      getNodesAux :: Z.Zookeeper -> String -> Either Z.ZKError [String] -> IO [Node]
      getNodesAux zh node maybeNodes = mapM (\n -> buildTree zh $ joinNodes node n) $ either (\_ -> []) id maybeNodes

      joinNodes n1 n2 = case (n1, n2) of
                           ("/", n2) -> "/" ++ n2
                           (n1,  n2) -> n1 ++ "/" ++ n2

printJson :: Node -> IO ()
printJson node = T.putStrLn $ T.decodeUtf8 $ encode $ node

printPlain :: Node -> IO ()
printPlain node = printPlainAux node 0
   where
      printPlainAux node level = do
         putStrLn $ formatNode node level
         mapM_ (\node -> printPlainAux node (level+1)) (maybe [] id $ nodeChildren node)

      formatNode node level = indent level $ (formatPath (nodePath node) (nodeName node)) ++ (formatValue $ nodeValue node)

      formatPath "/" name  = "/" ++ name
      formatPath path name = path ++ "/" ++ name

      formatValue Nothing = ""
      formatValue (Just "") = ""
      formatValue (Just value) = "\n\t" ++ value

toMaybe :: [a] -> Maybe [a]
toMaybe [] = Nothing
toMaybe list = Just list

indent :: Int -> String -> String
indent n val = (concat $ take (n*2) $ repeat " ") ++ val
