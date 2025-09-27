import { Outlet } from "react-router-dom";
import Header from "./components/layout/Header/Header";

function App() {
  return (
    <>
      <Header />
      <main className="main-container">
        <Outlet />
      </main>
    </>
  );
}

export default App;